import requests
import time
import socket
import random
import string

def get_ip_ports_from_api(url):
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            return data
        else:
            print(f"返回数据格式不是列表: {data}")
            return []
    except Exception as e:
        print(f"请求或解析数据时出错: {e}")
        return []

def reset_election(ip_port, replid):
    ip, _ = ip_port.split(":")
    url = f"http://{ip}:8080/keepers/election/reset"
    payload = {"replId": replid}
    try:
        resp = requests.post(url, json=payload, timeout=5)
        print(f"{url} POST返回状态码: {resp.status_code}, 响应: {resp.text}")
    except Exception as e:
        print(f"POST请求 {url} 时出错: {e}")

def random_str(length=4):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def redis_set_command(key, value):
    # 构造 RESP 协议的 SET 命令
    cmd = f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"
    return cmd.encode()

def send_redis_sets(ip, port, num=1000):
    try:
        s = socket.create_connection((ip, port), timeout=3)
        for i in range(1, num + 1):
            key = f"key_{i}"
            value = random_str(4)
            cmd = redis_set_command(key, value)
            s.sendall(cmd)
            resp = s.recv(1024)
            print(f"SET {key} {value} -> {resp.decode().strip()}")
        s.close()
        print(f"对 Redis {ip}:{port} 完成 {num} 个 SET 命令")
    except Exception as e:
        print(f"Redis {ip}:{port} 执行SET命令出错: {e}")

if __name__ == "__main__":
    clusters = [
        {"name": "xpipe-test-gap-allow-xsync", "replid": "786906"},
        {"name": "xpipe-test-gap-allow-psync", "replid": "786899"}
    ]
    base_url = "http://xpipe.ptjq.fx.uat.tripqate.com/api/keepers/UAT/{}/test1"

    redis_targets = [
        ("10.120.125.145", 20000),
        ("10.120.125.145", 20004)
    ]

    for cluster in clusters:
        api_url = base_url.format(cluster["name"])
        print(f"正在查询 cluster: {cluster['name']}")
        ip_ports = get_ip_ports_from_api(api_url)
        print(f"为 cluster {cluster['name']} 发送reset POST命令，并随后发送数据...")
        for entry in ip_ports:
            reset_election(entry, cluster["replid"])
            # 紧接着发送数据
            for ip, port in redis_targets:
                print(f"对 Redis {ip}:{port} 执行1000个 SET 命令...")
                send_redis_sets(ip, port, num=1000)
            print("等待30秒...")
            time.sleep(30)