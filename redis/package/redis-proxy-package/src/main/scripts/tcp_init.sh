#!/bin/bash
#
# tcp_init.sh — 容器内 TCP 内核调参
# ---------------------------------------------------------------------------
# 只调整容器 netns 内可生效的 3 项：
#   net.ipv4.tcp_rmem / tcp_wmem / tcp_congestion_control
# 写 /etc/sysctl.conf + sysctl -p，并对实际值做强校验(不符则 exit 1)。
# 所有输出带时间戳，写入 /opt/logs/<APPID>/tcp_init.log，同时保留控制台输出。
# ---------------------------------------------------------------------------

########################### 日志 ###########################
APPID=100013684
LOG_DIR=/opt/logs/${APPID}
TCP_LOG=${LOG_DIR}/tcp_init.log
mkdir -p "$LOG_DIR" 2>/dev/null

# 统一日志函数：每条都带执行时间；同时写文件与控制台
log() {
    local line
    line="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "$line"
    echo "$line" >>"$TCP_LOG" 2>/dev/null
}

########################### 期望值 ###########################
SYSCTL_CONF=/etc/sysctl.conf
TCP_RMEM="4096 87380 50485760"
TCP_WMEM="4096 87380 50485760"
CONGESTION_CONTROL="bbr"

log "===== tcp_init.sh start (uid=$(id -u)) ====="

########################### sudo 兼容 ###########################
if sudo -n true 2>/dev/null; then
    SUDO="sudo"
elif [ "$(id -u)" = "0" ]; then
    SUDO=""
else
    SUDO=""
    log "[WARN] 当前非 root 且无免密 sudo，sysctl.conf 写入/sysctl -p 可能失败(继续)"
fi

########################### Clean（删除旧行，幂等） ###########################
$SUDO sed -i '/net.ipv4.tcp_rmem/d'                $SYSCTL_CONF 2>/dev/null
$SUDO sed -i '/net.ipv4.tcp_wmem/d'                $SYSCTL_CONF 2>/dev/null
$SUDO sed -i '/net.ipv4.tcp_congestion_control/d'  $SYSCTL_CONF 2>/dev/null

# ---- 容器内不可调整/不需要的项，保留原 VM 写法但注释掉 ----
# net.core.* 在容器 netns 内不可见：
# $SUDO sed -i '/net.core.wmem_max/d'        $SYSCTL_CONF 2>/dev/null
# $SUDO sed -i '/net.core.rmem_max/d'        $SYSCTL_CONF 2>/dev/null
# $SUDO sed -i '/net.core.default_qdisc/d'   $SYSCTL_CONF 2>/dev/null

########################### Set（写入 /etc/sysctl.conf） ###########################
echo "net.ipv4.tcp_rmem = $TCP_RMEM"                        | $SUDO tee -a $SYSCTL_CONF >/dev/null
echo "net.ipv4.tcp_wmem = $TCP_WMEM"                        | $SUDO tee -a $SYSCTL_CONF >/dev/null
echo "net.ipv4.tcp_congestion_control = $CONGESTION_CONTROL" | $SUDO tee -a $SYSCTL_CONF >/dev/null
log "written to $SYSCTL_CONF: tcp_rmem / tcp_wmem / tcp_congestion_control"

# ---- 不调整项(注释，保留原 VM 写法参考) ----
# echo 'net.core.wmem_max=50485760'            | $SUDO tee -a $SYSCTL_CONF >/dev/null
# echo 'net.core.rmem_max=50485760'            | $SUDO tee -a $SYSCTL_CONF >/dev/null
# echo 'net.core.default_qdisc=fq'             | $SUDO tee -a $SYSCTL_CONF >/dev/null
# echo 'net.ipv4.ip_unprivileged_port_start=0' | $SUDO tee -a $SYSCTL_CONF >/dev/null

########################### Apply ###########################
if $SUDO sysctl -p >/dev/null 2>&1; then
    log "sysctl -p applied"
else
    log "[ERROR] sysctl -p 执行失败"
fi

########################### Check（强校验，失败即 exit 1） ###########################
log "checking tcp params..."
FAIL=0

# 按字段比较三元组(min default max)，忽略空格/制表符差异
check_triple() {
    local name="$1"; local file="$2"; local expect="$3"
    local actual a e
    actual=$(cat "$file" 2>/dev/null)
    a=$(echo "$actual" | awk '{print $1" "$2" "$3}')
    e=$(echo "$expect" | awk '{print $1" "$2" "$3}')
    log "$name = $actual"
    if [ "$a" != "$e" ]; then
        log "[ERROR] $name should be: $e, but: $a"
        FAIL=1
    fi
}

cc=$(cat /proc/sys/net/ipv4/tcp_congestion_control 2>/dev/null)
log "net.ipv4.tcp_congestion_control = $cc"
if [ "$cc" != "$CONGESTION_CONTROL" ]; then
    log "[ERROR] congestion_control should be $CONGESTION_CONTROL, but: $cc"
    FAIL=1
fi

check_triple "net.ipv4.tcp_rmem" /proc/sys/net/ipv4/tcp_rmem "$TCP_RMEM"
check_triple "net.ipv4.tcp_wmem" /proc/sys/net/ipv4/tcp_wmem "$TCP_WMEM"

if [ "$FAIL" -ne 0 ]; then
    log "[ERROR] tcp params not set correctly, exit 1"
    exit 1
fi

log "all tcp params verified ok"
exit 0
