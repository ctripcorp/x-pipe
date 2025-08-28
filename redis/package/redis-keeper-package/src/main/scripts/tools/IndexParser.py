import struct
import os
import argparse
import sys
import json
from typing import Dict, Any, List, IO, Union

# --- 常量定义 ---
SEPARATOR = b'\r\n'
INDEX_ENTRY_FORMAT = '>40sqqqqi2s'
INDEX_ENTRY_SIZE = struct.calcsize(INDEX_ENTRY_FORMAT)
assert INDEX_ENTRY_SIZE == 78, "计算的记录大小与预期不符"

def read_varint(f: IO[bytes]) -> Union[int, None]:
    """
    从文件流中读取一个VarInt。这是Java VarInt.getVarInt的Python实现。
    此版本兼容 Python 3.9 及更早版本。
    """
    result = 0
    shift = 0
    while True:
        byte_chunk = f.read(1)
        if not byte_chunk: return None
        byte = byte_chunk[0]
        result |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0: return result
        shift += 7
        if shift >= 64: raise ValueError("VarInt is too long or invalid.")

def parse_block_file(block_file_path: str, record: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    解析block文件的一个片段，该片段由一个IndexEntry记录定义。
    """
    if not os.path.exists(block_file_path):
        return []

    commands = []
    try:
        with open(block_file_path, 'rb') as f:
            f.seek(record['block_start_offset'])
            current_cmd_offset = record['cmd_start_offset']
            current_gno = record['start_gno']
            while f.tell() < record['block_end_offset']:
                offset_delta = read_varint(f)
                if offset_delta is None: break
                current_cmd_offset += offset_delta
                commands.append({
                    'gno': current_gno,
                    'offset_delta': offset_delta,
                    'cmd_offset': current_cmd_offset
                })
                current_gno += 1
    except Exception as e:
        print(f"解析block文件 {os.path.basename(block_file_path)} 时发生错误: {e}")
    return commands

def parse_index_file(file_path: str) -> Union[Dict[str, Any], None]:
    """解析单个.idx索引文件。"""
    results = {"gtid_set": "", "records": []}
    try:
        with open(file_path, 'rb') as f:
            len_bytes = f.read(8)
            if len(len_bytes) < 8: return None
            gtid_length = struct.unpack('>q', len_bytes)[0]
            if gtid_length > 0:
                gtid_bytes = f.read(gtid_length)
                if len(gtid_bytes) < gtid_length: return None
                results['gtid_set'] = gtid_bytes.decode('utf-8')
            while True:
                chunk = f.read(INDEX_ENTRY_SIZE)
                if len(chunk) < INDEX_ENTRY_SIZE: break
                unpacked_data = struct.unpack(INDEX_ENTRY_FORMAT, chunk)
                if unpacked_data[6] != SEPARATOR: break
                record = {
                    'uuid': unpacked_data[0].decode('utf-8').strip('\x00'),
                    'start_gno': unpacked_data[1],
                    'cmd_start_offset': unpacked_data[2],
                    'block_start_offset': unpacked_data[3],
                    'block_end_offset': unpacked_data[4],
                    'size': unpacked_data[5]
                }
                results['records'].append(record)
    except Exception as e:
        print(f"解析索引文件 {os.path.basename(file_path)} 时发生严重错误: {e}")
        return None
    return results

def main():
    parser = argparse.ArgumentParser(
        description="解析X-Pipe Keeper的索引(.idx)和块(.block)文件。支持单个文件或目录。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "input_path",
        help="要解析的索引文件(.idx)或包含索引文件的目录的路径。"
    )
    parser.add_argument(
        "--entry",
        type=int,
        default=None,
        metavar='N',
        help="只解析并显示指定序号的IndexEntry (从1开始计数)。\n如果未提供，则显示所有条目。"
    )
    # 【关键新增点】添加 --no-block 开关
    parser.add_argument(
        "--no-block",
        action="store_true",
        help="仅解析索引(.idx)文件，不读取关联的块(.block)文件。\n此选项可以加快速度并简化输出。"
    )

    args = parser.parse_args()
    input_path = args.input_path
    target_entry_index = args.entry
    parse_blocks = not args.no_block # 如果提供了--no-block，则此值为False

    if target_entry_index is not None and target_entry_index <= 0:
        print("错误: --entry 参数必须是一个正整数 (例如: 1, 2, 3...)。"); sys.exit(1)

    if not os.path.exists(input_path):
        print(f"错误: 路径不存在 -> '{input_path}'"); sys.exit(1)

    files_to_process = []
    base_dir = ""
    if os.path.isdir(input_path):
        base_dir = input_path
        for filename in sorted(os.listdir(base_dir)):
            if filename.startswith("index_") and not filename.endswith(".py"):
                files_to_process.append(os.path.join(base_dir, filename))
    elif os.path.isfile(input_path):
        base_dir = os.path.dirname(input_path)
        files_to_process.append(input_path)
    else:
        print(f"错误: 路径既不是文件也不是目录 -> '{input_path}'"); sys.exit(1)

    if not files_to_process:
        print("未找到任何需要解析的索引文件。"); sys.exit(0)

    print(f"准备解析 {len(files_to_process)} 个索引文件...\n")

    for i, index_file_path in enumerate(files_to_process):
        print("=" * 80)
        print(f"正在处理文件: {os.path.basename(index_file_path)}")

        parsed_index = parse_index_file(index_file_path)

        if parsed_index:
            print(f"--- GTID Set: {parsed_index['gtid_set'] or '（空）'} ---")

            found_target = False
            for j, record in enumerate(parsed_index['records']):
                current_entry_index = j + 1
                if target_entry_index is not None and current_entry_index != target_entry_index:
                    continue

                found_target = True

                # 【关键逻辑】根据 parse_blocks 变量决定执行路径
                if parse_blocks:
                    # 完整模式：解析block文件
                    block_filename = os.path.basename(index_file_path).replace("index_", "block_", 1)
                    block_file_path = os.path.join(base_dir, block_filename)

                    commands_in_block = parse_block_file(block_file_path, record)
                    record['commands'] = commands_in_block

                    print(f"\n  --- IndexEntry {current_entry_index} (关联Block文件: {block_filename}) ---")
                    if not os.path.exists(block_file_path):
                        print(f"    警告: Block文件未找到于路径: {block_file_path}")
                    print(f"    (在Block文件中找到 {len(commands_in_block)} 个VarInt增量)")
                    print(json.dumps(record, indent=4))
                else:
                    # 快速模式：只显示索引信息
                    print(f"\n  --- IndexEntry {current_entry_index} ---")
                    print(json.dumps(record, indent=4))

            if target_entry_index is not None and not found_target:
                print(f"\n警告: 在文件 {os.path.basename(index_file_path)} 中未找到第 {target_entry_index} 条IndexEntry。该文件共有 {len(parsed_index['records'])} 条记录。")

        else:
            print(f"--- 解析失败: {os.path.basename(index_file_path)} ---")

    print("\n" + "=" * 80)
    print("所有任务完成。")

if __name__ == '__main__':
    main()
