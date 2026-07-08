#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import struct
import os
import argparse
import sys
import json

# ---------- 常量 ----------
# v1 常量
SEPARATOR = b'\r\n'
INDEX_ENTRY_FORMAT_V1 = '>40sqqqqi2s'
INDEX_ENTRY_SIZE_V1 = struct.calcsize(INDEX_ENTRY_FORMAT_V1)  # 78

# v2 常量
MAGIC = 0x47494458
VERSION_V2 = 2
INDEX_ENTRY_FORMAT_V2 = '>BBH40sqqqqqiq'
INDEX_ENTRY_SIZE_V2 = struct.calcsize(INDEX_ENTRY_FORMAT_V2)  # 96

TYPE_GTID = 0
TYPE_ZONE = 1

# 兼容 Python 2
PY2 = sys.version_info[0] == 2

def read_varint(f):
    """读取 VarInt，兼容 Python 2/3"""
    result = 0
    shift = 0
    while True:
        chunk = f.read(1)
        if not chunk:
            return None
        byte = ord(chunk) if PY2 else chunk[0]
        result |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            return result
        shift += 7
        if shift >= 64:
            raise ValueError("VarInt too long or invalid")

def parse_block_file(block_file_path, record):
    """解析 block 文件的一个片段"""
    if not os.path.exists(block_file_path):
        return []
    commands = []
    try:
        with open(block_file_path, 'rb') as f:
            f.seek(record['block_start_offset'])
            current_offset = record['cmd_start_offset']
            current_gno = record['start_gno']
            while f.tell() < record['block_end_offset']:
                delta = read_varint(f)
                if delta is None:
                    break
                current_offset += delta
                commands.append({
                    'gno': current_gno,
                    'offset_delta': delta,
                    'cmd_offset': current_offset
                })
                current_gno += 1
    except Exception as e:
        print("解析 block 文件 %s 出错: %s" % (os.path.basename(block_file_path), e))
    return commands

# ---------- V1 解析 ----------
def parse_v1_index_file(file_path):
    """解析 v1 索引文件 (index_*)"""
    results = {"gtid_set": "", "entries": []}
    try:
        with open(file_path, 'rb') as f:
            len_bytes = f.read(8)
            if len(len_bytes) < 8:
                return None
            gtid_length = struct.unpack('>q', len_bytes)[0]
            if gtid_length > 0:
                gtid_bytes = f.read(gtid_length)
                if len(gtid_bytes) < gtid_length:
                    return None
                results['gtid_set'] = gtid_bytes.decode('utf-8')
            while True:
                chunk = f.read(INDEX_ENTRY_SIZE_V1)
                if len(chunk) < INDEX_ENTRY_SIZE_V1:
                    break
                unpacked = struct.unpack(INDEX_ENTRY_FORMAT_V1, chunk)
                if unpacked[6] != SEPARATOR:
                    break
                entry = {
                    'type': 0,                     # GTID
                    'flags': 0,
                    'reserved': 0,
                    'uuid': unpacked[0].decode('utf-8').strip('\x00'),
                    'start_gno': unpacked[1],
                    'cmd_start_offset': unpacked[2],
                    'cmd_end_offset': 0,           # v1 无此字段
                    'block_start_offset': unpacked[3],
                    'block_end_offset': unpacked[4],
                    'size': unpacked[5],
                    'ext_reserved': 0
                }
                results['entries'].append(entry)
    except Exception as e:
        print("解析 v1 索引文件 %s 失败: %s" % (os.path.basename(file_path), e))
        return None
    return results

# ---------- V2 解析 ----------
def parse_v2_index_file(file_path):
    """解析 v2 索引文件 (indexv2_*)"""
    results = {"gtid_set": "", "entries": []}
    try:
        with open(file_path, 'rb') as f:
            # Header
            magic_bytes = f.read(4)
            if len(magic_bytes) < 4:
                return None
            magic = struct.unpack('>I', magic_bytes)[0]
            if magic != MAGIC:
                print("  错误: 无效魔数 0x%08X" % magic)
                return None
            ver_bytes = f.read(4)
            if len(ver_bytes) < 4:
                return None
            version = struct.unpack('>I', ver_bytes)[0]
            if version != VERSION_V2:
                print("  错误: 不支持版本 %d" % version)
                return None
            gtid_len_bytes = f.read(8)
            if len(gtid_len_bytes) < 8:
                return None
            gtid_len = struct.unpack('>q', gtid_len_bytes)[0]
            if gtid_len > 0:
                gtid_bytes = f.read(gtid_len)
                if len(gtid_bytes) < gtid_len:
                    return None
                results['gtid_set'] = gtid_bytes.decode('utf-8')

            # Entries
            while True:
                chunk = f.read(INDEX_ENTRY_SIZE_V2)
                if len(chunk) < INDEX_ENTRY_SIZE_V2:
                    break
                unpacked = struct.unpack(INDEX_ENTRY_FORMAT_V2, chunk)
                entry = {
                    'type': unpacked[0],
                    'flags': unpacked[1],
                    'reserved': unpacked[2],
                    'uuid': unpacked[3].decode('utf-8').rstrip('\x00'),
                    'start_gno': unpacked[4],
                    'cmd_start_offset': unpacked[5],
                    'cmd_end_offset': unpacked[6],
                    'block_start_offset': unpacked[7],
                    'block_end_offset': unpacked[8],
                    'size': unpacked[9],
                    'ext_reserved': unpacked[10]
                }
                results['entries'].append(entry)
    except Exception as e:
        print("解析 v2 索引文件 %s 失败: %s" % (os.path.basename(file_path), e))
        return None
    return results

# ---------- 辅助函数 ----------
def get_block_filename(index_filename):
    """根据索引文件名构造 block 文件名"""
    base = os.path.basename(index_filename)
    if base.startswith('indexv2_'):
        return 'blockv2_' + base[len('indexv2_'):]
    elif base.startswith('index_'):
        return 'block_' + base[len('index_'):]
    # 未知格式，尝试简单替换
    return base.replace('index', 'block', 1)

def main():
    parser = argparse.ArgumentParser(
        description="解析 XPipe Keeper 索引文件 (v1: index_*, v2: indexv2_*) 及关联 block 文件。"
    )
    parser.add_argument("input_path", help="索引文件或目录")
    parser.add_argument("--entry", type=int, default=None, help="只显示指定序号的条目")
    parser.add_argument("--no-block", action="store_true", help="不解析 block 文件")

    args = parser.parse_args()
    input_path = args.input_path
    target_entry = args.entry
    parse_blocks = not args.no_block

    if target_entry is not None and target_entry <= 0:
        print("错误: --entry 必须为正整数"); sys.exit(1)
    if not os.path.exists(input_path):
        print("错误: 路径不存在 -> '%s'" % input_path); sys.exit(1)

    # 收集文件
    files_to_process = []
    base_dir = ""
    if os.path.isdir(input_path):
        base_dir = input_path
        for fname in sorted(os.listdir(base_dir)):
            if fname.startswith("index") and not fname.endswith(".py"):
                files_to_process.append(os.path.join(base_dir, fname))
    else:
        base_dir = os.path.dirname(input_path)
        files_to_process.append(input_path)

    if not files_to_process:
        print("未找到任何索引文件。"); sys.exit(0)

    print("准备解析 %d 个索引文件...\n" % len(files_to_process))

    for i, index_path in enumerate(files_to_process):
        print("=" * 80)
        base_name = os.path.basename(index_path)
        print("正在处理: %s" % base_name)

        # 根据文件名前缀选择解析器
        if base_name.startswith('indexv2_'):
            parsed = parse_v2_index_file(index_path)
        elif base_name.startswith('index_'):
            parsed = parse_v1_index_file(index_path)
        else:
            print("  跳过未知前缀文件")
            continue

        if parsed is None:
            print("--- 解析失败 ---")
            continue

        gtid_str = parsed['gtid_set'] or '(空)'
        print("--- GTID Set: %s ---" % gtid_str)

        block_path = None
        if parse_blocks:
            block_filename = get_block_filename(base_name)
            block_path = os.path.join(base_dir, block_filename)

        found = False
        for j, entry in enumerate(parsed['entries']):
            entry_num = j + 1
            if target_entry is not None and entry_num != target_entry:
                continue
            found = True

            entry_type = "GTID" if entry['type'] == TYPE_GTID else "ZONE"
            print("\n  --- IndexEntry %d (类型: %s) ---" % (entry_num, entry_type))

            if entry['type'] == TYPE_GTID and parse_blocks and block_path:
                cmds = parse_block_file(block_path, entry)
                entry['commands'] = cmds
                print("    (关联 Block: %s, 命令数: %d)" % (os.path.basename(block_path), len(cmds)))
                if not os.path.exists(block_path):
                    print("    警告: Block 文件未找到: %s" % block_path)
            elif entry['type'] == TYPE_ZONE and parse_blocks:
                print("    (ZONE 条目，无 block 文件)")

            print(json.dumps(entry, indent=4, ensure_ascii=False))

        if target_entry is not None and not found:
            print("\n警告: 未找到第 %d 条 IndexEntry (共 %d 条)" % (target_entry, len(parsed['entries'])))

    print("\n" + "=" * 80)
    print("所有任务完成。")

if __name__ == '__main__':
    main()