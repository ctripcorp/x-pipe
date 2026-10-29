#!/bin/bash
# tcp_init.sh — 容器内 TCP 内核调参(tcp_rmem / tcp_wmem / tcp_congestion_control)

LOG=/tmp/tcp_init.log
SYSCTL_CONF=/etc/sysctl.conf
TCP_RMEM="4096 87380 50485760"
TCP_WMEM="4096 87380 50485760"
CC="bbr"

# 创建日志文件
touch "$LOG"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ===== tcp_init.sh start (uid=$(id -u)) =====" >> "$LOG"

# 写入 /etc/sysctl.conf(先删旧行再追加，幂等)
sed -i '/net.ipv4.tcp_rmem/d'               "$SYSCTL_CONF"
sed -i '/net.ipv4.tcp_wmem/d'               "$SYSCTL_CONF"
sed -i '/net.ipv4.tcp_congestion_control/d' "$SYSCTL_CONF"
echo "net.ipv4.tcp_rmem = $TCP_RMEM"               >> "$SYSCTL_CONF"
echo "net.ipv4.tcp_wmem = $TCP_WMEM"               >> "$SYSCTL_CONF"
echo "net.ipv4.tcp_congestion_control = $CC"       >> "$SYSCTL_CONF"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] written to $SYSCTL_CONF" >> "$LOG"

# 应用
sysctl -p >> "$LOG" 2>&1
echo "[$(date '+%Y-%m-%d %H:%M:%S')] sysctl -p done" >> "$LOG"

# 校验实际值
echo "[$(date '+%Y-%m-%d %H:%M:%S')] tcp_congestion_control = $(cat /proc/sys/net/ipv4/tcp_congestion_control)" >> "$LOG"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] tcp_rmem = $(cat /proc/sys/net/ipv4/tcp_rmem)" >> "$LOG"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] tcp_wmem = $(cat /proc/sys/net/ipv4/tcp_wmem)" >> "$LOG"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ===== tcp_init.sh end =====" >> "$LOG"
