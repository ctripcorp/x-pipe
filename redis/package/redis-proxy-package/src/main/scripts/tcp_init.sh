#!/bin/bash
#
# ---------------------------------------------------------------------------

########################### 期望值 ###########################
SYSCTL_CONF=/etc/sysctl.conf
TCP_RMEM="4096 87380 50485760"
TCP_WMEM="4096 87380 50485760"
CONGESTION_CONTROL="bbr"

########################### sudo 兼容 ###########################
if sudo -n true 2>/dev/null; then
    SUDO="sudo"
elif [ "$(id -u)" = "0" ]; then
    SUDO=""
else
    SUDO=""
    echo "[init.sh][WARN] 当前非 root 且无免密 sudo，sysctl.conf 写入/sysctl -p 可能失败（继续，不中断启动）"
fi

########################### Clean（删除旧行，幂等） ###########################
# 用 sudo 执行 sed -i，避免 deploy 无权写 /etc/sysctl.conf
$SUDO sed -i '/net.ipv4.tcp_rmem/d'                $SYSCTL_CONF 2>/dev/null
$SUDO sed -i '/net.ipv4.tcp_wmem/d'                $SYSCTL_CONF 2>/dev/null
$SUDO sed -i '/net.ipv4.tcp_congestion_control/d'  $SYSCTL_CONF 2>/dev/null

# ---- 以下为容器内不可调整/不需要的项，保留原 VM 写法但注释掉 ----
# net.core.* 在容器 netns 内不可见（No such file or directory）：
# $SUDO sed -i '/net.core.wmem_max/d'        $SYSCTL_CONF 2>/dev/null
# $SUDO sed -i '/net.core.rmem_max/d'        $SYSCTL_CONF 2>/dev/null
# $SUDO sed -i '/net.core.default_qdisc/d'   $SYSCTL_CONF 2>/dev/null

########################### Set（写入 /etc/sysctl.conf） ###########################
# 通过 sudo tee -a 追加写入(deploy 无法直接 >> root 文件)
echo "net.ipv4.tcp_rmem = $TCP_RMEM"                        | $SUDO tee -a $SYSCTL_CONF >/dev/null
echo "net.ipv4.tcp_wmem = $TCP_WMEM"                        | $SUDO tee -a $SYSCTL_CONF >/dev/null
echo "net.ipv4.tcp_congestion_control = $CONGESTION_CONTROL" | $SUDO tee -a $SYSCTL_CONF >/dev/null

# ---- 不调整项（注释，保留原 VM 写法参考）----
# echo 'net.core.wmem_max=50485760'         | $SUDO tee -a $SYSCTL_CONF >/dev/null
# echo 'net.core.rmem_max=50485760'         | $SUDO tee -a $SYSCTL_CONF >/dev/null
# echo 'net.core.default_qdisc=fq'          | $SUDO tee -a $SYSCTL_CONF >/dev/null
# echo 'net.ipv4.ip_unprivileged_port_start=0' | $SUDO tee -a $SYSCTL_CONF >/dev/null

########################### Apply ###########################
if $SUDO sysctl -p >/dev/null 2>&1; then
    echo "[init.sh] sysctl -p applied"
else
    echo "[init.sh][ERROR] sysctl -p 执行失败"
fi

########################### Check（强校验，失败即 exit 1） ###########################
# 任何一项实际值与期望不符，脚本以非 0 退出，使 startup.sh 中止启动。
echo ''
echo '[init.sh] Checking tcp params...'

FAIL=0

# 按字段比较三元组(min default max)，忽略空格/制表符差异
check_triple() {
    local name="$1"; local file="$2"; local expect="$3"
    local actual
    actual=$(cat "$file" 2>/dev/null)
    echo "$name = $actual"
    # 用 awk 把两边都压成空格分隔再比较
    local a e
    a=$(echo "$actual" | awk '{print $1" "$2" "$3}')
    e=$(echo "$expect" | awk '{print $1" "$2" "$3}')
    if [ "$a" != "$e" ]; then
        echo "[init.sh][ERROR] $name should be: $e, but: $a"
        FAIL=1
    fi
}

cc=$(cat /proc/sys/net/ipv4/tcp_congestion_control 2>/dev/null)
echo "net.ipv4.tcp_congestion_control = $cc"
if [ "$cc" != "$CONGESTION_CONTROL" ]; then
    echo "[init.sh][ERROR] congestion_control should be $CONGESTION_CONTROL, but: $cc"
    FAIL=1
fi

check_triple "net.ipv4.tcp_rmem" /proc/sys/net/ipv4/tcp_rmem "$TCP_RMEM"
check_triple "net.ipv4.tcp_wmem" /proc/sys/net/ipv4/tcp_wmem "$TCP_WMEM"

if [ "$FAIL" -ne 0 ]; then
    echo "[init.sh][ERROR] tcp params not set correctly, exit 1"
    exit 1
fi

echo "[init.sh] all tcp params verified ok"
exit 0
