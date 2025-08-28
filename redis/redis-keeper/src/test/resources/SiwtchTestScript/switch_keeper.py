import socket
import requests
import time
import subprocess
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("redis_cluster_check")

def get_redis_instances(cluster_name):
    api_url = f"http://credis.arch.uat.qa.nt.ctripcorp.com/opsapi/GetOpsClusterV2?clusterName={cluster_name}"
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        instances = []
        for group in data["Groups"]:
            for instance in group["Instances"]:
                instances.append({
                    "IPAddress": instance["IPAddress"],
                    "Port": instance["Port"],
                    "Env": instance.get("Env", None),
                })
        return instances
    except Exception as e:
        logger.error(f"Error fetching data from API: {e}")
        return None


def send_redis_command(ip, port, command, timeout=3):
    s = None
    try:
        s = socket.create_connection((ip, port), timeout=timeout)
        s.settimeout(timeout)

        def send_raw(cmd):
            parts = cmd.strip().split()
            resp = f"*{len(parts)}\r\n"
            for part in parts:
                resp += f"${len(part)}\r\n{part}\r\n"
            s.sendall(resp.encode())

        send_raw(command)

        def recvline(sock):
            line = b""
            while True:
                char = sock.recv(1)
                if not char:
                    break
                line += char
                if line[-2:] == b'\r\n':
                    break
            return line

        first_line = recvline(s)
        if not first_line:
            return ""

        prefix = first_line[:1]
        if prefix == b'$':
            length = int(first_line[1:-2])
            if length == -1:
                return None
            data = b""
            while len(data) < length:
                chunk = s.recv(length - len(data))
                if not chunk:
                    break
                data += chunk
            s.recv(2)
            return data.decode('utf-8', errors='replace')
        else:
            return first_line.decode('utf-8', errors='replace')
    except Exception as e:
        logger.error(f"Error communicating with Redis at {ip}:{port} - {e}")
        return None
    finally:
        if s:
            try:
                s.close()
            except Exception:
                pass


def parse_info_fields(response_str):
    if not response_str:
        return None, None, None, None
    role = None
    master_repl_offset = None
    slave_repl_offset = None
    gtid_set = None
    gtid_lost = None
    for line in response_str.splitlines():
        if line.startswith("role:"):
            role = line[len("role:"):].strip()
        elif line.startswith("master_repl_offset:"):
            try:
                master_repl_offset = int(line[len("master_repl_offset:"):].strip())
            except ValueError:
                pass
        elif line.startswith("slave_repl_offset:"):
            try:
                slave_repl_offset = int(line[len("slave_repl_offset:"):].strip())
            except ValueError:
                pass
        elif line.startswith("gtid_set:"):
            gtid_set = line[len("gtid_set:"):].strip()
        elif line.startswith("gtid_lost:"):
            gtid_lost = line[len("gtid_lost:"):].strip()
    repl_offset = master_repl_offset if role == "master" else slave_repl_offset
    return role, repl_offset, gtid_set, gtid_lost


def set_master_acl_readonly(master_host, master_port):
    return send_redis_command(master_host, master_port, "ACL SETUSER default on +@read -@write")


def restore_master_acl_all(master_host, master_port):
    return send_redis_command(master_host, master_port, "ACL SETUSER default on +@all")


def find_master_and_env(instances):
    master_info = None
    master_env = None
    for instance in instances:
        host = instance["IPAddress"]
        port = instance["Port"]
        info = send_redis_command(host, port, "INFO replication")
        role, _, _, _ = parse_info_fields(info)
        if role == "master":
            master_info = instance
            master_env = instance.get("Env", None)
            break
    return master_info, master_env


def get_instances_with_env(instances, env):
    return [inst for inst in instances if inst.get("Env", None) == env]


def check_cluster_repl_gtid(instances, readonly_seconds=2):
    master_info, _ = find_master_and_env(instances)
    if not master_info:
        logger.error("没有找到master节点，无法进行ACL操作和对比。")
        return

    master_host = master_info["IPAddress"]
    master_port = master_info["Port"]

    acl_res = set_master_acl_readonly(master_host, master_port)
    logger.info(f"设置 master {master_host}:{master_port} 只读: {acl_res}")
    time.sleep(readonly_seconds)

    try:
        all_offsets = set()
        all_gtid_sets = set()
        gtid_losts_all_empty = True

        node_infos = []
        for instance in instances:
            host = instance["IPAddress"]
            port = instance["Port"]

            info = send_redis_command(host, port, "INFO")
            role, offset, gtid_set, gtid_lost = parse_info_fields(info)
            node_infos.append({
                "host": host,
                "port": port,
                "role": role,
                "repl_offset": offset,
                "gtid_set": gtid_set,
                "gtid_lost": gtid_lost
            })
            if offset is not None:
                all_offsets.add(offset)
            if gtid_set is not None:
                all_gtid_sets.add(','.join(sorted(map(str.strip, gtid_set.split(',')))))
            if gtid_lost not in (None, "", "nil"):
                gtid_losts_all_empty = False

            logger.info(f"Redis {host}:{port} (role: {role}) repl_offset: {offset}  gtid_set: {gtid_set}  gtid_lost: {gtid_lost}")

        if len(all_offsets) != 1:
            logger.error(f"\n断言失败：存在不同的repl_offset！所有repl_offset集合: {all_offsets}")
            logger.error("流程终止。")
            return

        if len(all_gtid_sets) != 1:
            logger.error(f"\n断言失败：存在不同的gtid_set！所有gtid_set集合: {all_gtid_sets}")
            logger.error("流程终止。")
            return

        if not gtid_losts_all_empty:
            logger.error("\ngtid_lost 断言失败：有节点的 gtid_lost 非空！")
            logger.error("流程终止。")
            return

        logger.info("\n所有节点repl_offset、gtid_set一致，且gtid_lost均为空，可以继续后续流程。")
    finally:
        recover_res = restore_master_acl_all(master_host, master_port)
        logger.info(f"\n恢复 master {master_host}:{master_port} 写权限: {recover_res}")


def run_redis_full_check_on_slaves(instances, full_check_bin="redis-full-check", full_check_extra_args=None):
    logger.info("\n==== 开始对所有slave进行redis-full-check一致性校验 ====")
    master_info, _ = find_master_and_env(instances)
    if not master_info:
        logger.warning("未找到master节点，跳过full-check。")
        return

    master_host = master_info["IPAddress"]
    master_port = master_info["Port"]

    slave_nodes = []
    for inst in instances:
        info = send_redis_command(inst["IPAddress"], inst["Port"], "INFO replication")
        role, _, _, _ = parse_info_fields(info)
        if role == "slave":
            slave_nodes.append(inst)

    if not slave_nodes:
        logger.warning("未找到slave节点，跳过full-check。")
        return

    for slave in slave_nodes:
        slave_host = slave["IPAddress"]
        slave_port = slave["Port"]
        logger.info(f"\n对比 master {master_host}:{master_port} <-> slave {slave_host}:{slave_port} ...")
        cmd = [
            full_check_bin,
            "--source", f"{master_host}:{master_port}",
            "--target", f"{slave_host}:{slave_port}",
            "--parallel", "4",
        ]
        if full_check_extra_args:
            cmd.extend(full_check_extra_args)
        logger.info("运行命令：%s", " ".join(cmd))
        try:
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=600)
            logger.info(f"redis-full-check 结果:\n{result.stdout}\n{result.stderr}")
        except Exception as e:
            logger.error(f"执行redis-full-check时发生异常: {e}")


def get_keeper_ip_ports(cluster_name, shard="test1"):
    base_url = f"http://xpipe.ptjq.fx.uat.tripqate.com/api/keepers/UAT/{cluster_name}/{shard}"
    try:
        response = requests.get(base_url, timeout=5)
        response.raise_for_status()
        ip_ports = response.json()
        if isinstance(ip_ports, list):
            return ip_ports
        else:
            logger.error(f"keeper接口返回不是列表: {ip_ports}")
            return []
    except Exception as e:
        logger.error(f"keeper接口请求或解析出错: {e}")
        return []


def keeper_reset(ip_port, replid):
    ip, _ = ip_port.split(":")
    url = f"http://{ip}:8080/keepers/election/reset"
    payload = {"replId": replid}
    try:
        resp = requests.post(url, json=payload, timeout=5)
        logger.info(f"{url} POST返回状态码: {resp.status_code}, 响应: {resp.text}")
        return resp.status_code == 200
    except Exception as e:
        logger.error(f"POST请求 {url} 时出错: {e}")
        return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Check Redis cluster repl_offset/gtid_set/gtid_lost consistency and keeper reset on Env, and run redis-full-check on slaves.")
    parser.add_argument("--cluster", type=str, default="xpipe-test-gap-allow-xsync", help="Redis cluster name")
    parser.add_argument("--keeper-replid", type=str, default= "786906", required=True, help="keeper replid, e.g. 786906")
    parser.add_argument("--shard", type=str, default="xpipe-test-gap-allow-xsync_1", help="keeper shard")
    parser.add_argument("--debug-sleep-interval", type=int, default=60, help="Interval between keeper reset (seconds, default 600=10min)")
    parser.add_argument("--readonly-seconds", type=int, default=5, help="Readonly duration in seconds for master ACL")
    parser.add_argument("--repeat", type=int, default=1, help="How many times to repeat reset+check (default 1)")
    parser.add_argument("--full-check", default=True ,action="store_true", help="是否对所有slave进行redis-full-check主从一致性校验")
    parser.add_argument("--full-check-bin", type=str, default="./redis-full-check", help="redis-full-check二进制路径")
    parser.add_argument("--full-check-extra-args", type=str, default="", help="redis-full-check额外参数（空格分隔）")
    args = parser.parse_args()

    instances = get_redis_instances(args.cluster)
    if not instances:
        logger.error("没有获取到Redis实例信息。")
        return

    for round_num in range(1, args.repeat + 1):
        logger.info(f"\n==== 第{round_num}轮 keeper reset + 检查 ====")

        # 1. 获取 keeper ip:port
        keeper_ip_ports = get_keeper_ip_ports(args.cluster, args.shard)
        if not keeper_ip_ports:
            logger.error("没有获取到 keeper ip:port 信息，终止。")
            break

        # 2. 给所有 keeper 节点 reset
        for ip_port in keeper_ip_ports:
            ip, port = ip_port.split(":")
            info_result = send_redis_command(ip, port, "info")
            if "ACTIVE"in info_result:
                logger.info(f"正在重置 keeper {ip_port} ...")
                keeper_reset(ip_port, args.keeper_replid)
                break

        logger.info("等待一段时间后进行repl/gtid一致性检查...")
        time.sleep(20)  # 你可以根据实际情况调整

        check_cluster_repl_gtid(instances, readonly_seconds=args.readonly_seconds)

        if args.full_check:
            run_redis_full_check_on_slaves(
                instances,
                full_check_bin=args.full_check_bin,
                full_check_extra_args=args.full_check_extra_args.split() if args.full_check_extra_args else None
            )

        if round_num < args.repeat:
            logger.info(f"等待 {args.debug_sleep_interval} 秒后开始下一轮...")
            time.sleep(args.debug_sleep_interval)

if __name__ == "__main__":
    main()