#!/usr/bin/env bash
#
# sudo_before_shutdown.sh
# ---------------------------------------------------------------------------

APPID=100013684
LOG_DIR=/opt/logs/${APPID}
BEFORE_SHUTDOWN_LOG=${LOG_DIR}/before_shutdown.log

# 本脚本所在目录(init.sh 与本脚本同目录)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_INIT="${SCRIPT_DIR}/init.sh"

# 目标：持久化卷上的 tcp_param 目录
TCP_PARAM_DIR=/opt/data/${APPID}/tcp_param
DST_INIT="${TCP_PARAM_DIR}/init.sh"

mkdir -p "$LOG_DIR"
echo "" >>"$BEFORE_SHUTDOWN_LOG"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ======= START sudo_before_shutdown.sh (uid=$(id -u))" >>"$BEFORE_SHUTDOWN_LOG"

# 1. 源 init.sh 必须存在
if [ ! -f "$SRC_INIT" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: source init.sh not found: $SRC_INIT" >>"$BEFORE_SHUTDOWN_LOG"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ======= END sudo_before_shutdown.sh (rc=1)" >>"$BEFORE_SHUTDOWN_LOG"
    exit 1
fi

# 2. 复制到 EBS 持久化目录
mkdir -p "$TCP_PARAM_DIR"
if ! cp -f "$SRC_INIT" "$DST_INIT"; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: copy init.sh failed: $SRC_INIT -> $DST_INIT" >>"$BEFORE_SHUTDOWN_LOG"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ======= END sudo_before_shutdown.sh (rc=1)" >>"$BEFORE_SHUTDOWN_LOG"
    exit 1
fi
chmod 755 "$DST_INIT"
# proxy 由 deploy 运行，EBS 目录归属 deploy，便于其读取
chown -R deploy:deploy "$TCP_PARAM_DIR" 2>/dev/null
echo "[$(date '+%Y-%m-%d %H:%M:%S')] copied init.sh -> $DST_INIT" >>"$BEFORE_SHUTDOWN_LOG"

# 3. 以 root 执行 EBS 上的 init.sh(完成实际 TCP 调参)
echo "[$(date '+%Y-%m-%d %H:%M:%S')] run tcp param script: $DST_INIT" >>"$BEFORE_SHUTDOWN_LOG"
bash "$DST_INIT" >>"$BEFORE_SHUTDOWN_LOG" 2>&1
rc=$?

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ======= END sudo_before_shutdown.sh (rc=$rc)" >>"$BEFORE_SHUTDOWN_LOG"
exit $rc
