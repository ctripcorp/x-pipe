#change ulimit config
ULIMIT_CONFIG=/etc/security/limits.d/20-nproc.conf
LIMIT_NUM=1048576

sed -i "s/- nofile.*/- nofile $LIMIT_NUM/" $ULIMIT_CONFIG
sed -i "s/- nproc.*/- nproc $LIMIT_NUM/" $ULIMIT_CONFIG
sed -i "s/- sigpending.*/- sigpending $LIMIT_NUM/" $ULIMIT_CONFIG

#change pid max
PID_CONFIG=/etc/sysctl.conf
if [ `grep kernel.pid_max $PID_CONFIG | wc -l` -gt 0 ]; then
    sed -i "s/kernel.pid_max.*/kernel.pid_max = $LIMIT_NUM/" $PID_CONFIG
else
    echo "kernel.pid_max = $LIMIT_NUM" >> /etc/sysctl.conf
fi

#change systemd
SYSTEMD_CONFIG=/etc/systemd/system.conf
sed -i "s/^DefaultLimitNOFILE.*/DefaultLimitNOFILE=$LIMIT_NUM/" $SYSTEMD_CONFIG
sed -i "s/^DefaultLimitNPROC.*/DefaultLimitNPROC=$LIMIT_NUM/" $SYSTEMD_CONFIG

#cat /etc/security/limits.d/20-nproc.conf
#cat /etc/sysctl.conf
#cat /etc/systemd/system.conf

#ulimit -a
#cat /proc/sys/kernel/pid_max