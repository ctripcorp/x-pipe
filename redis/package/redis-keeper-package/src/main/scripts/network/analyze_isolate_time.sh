DIR=`dirname $0`
#LOGPATH="$DIR/logs"
SSHCMD_PATH="$DIR./sshcmd"
SSHCMD_LOG_PATH="$SSHCMD_PATH/logs"
IPLIST_PATH="$DIR/iplist"
operation=$1

if [ "$operation" == "recovery" ]; then
    $SSHCMD_PATH/ssh_operate.sh "cat remote_cron/recovery_cron.log" $IPLIST_PATH
else
    $SSHCMD_PATH/ssh_operate.sh "cat remote_cron/isolate_cron.log" $IPLIST_PATH
fi
start_min=5020642181884
end_max=0
interval_max=0
interval_max_ip=''

while read ip
do
    start=`tail -n 3 $SSHCMD_LOG_PATH/$ip.log | head -n 1`
    if [ $(($start)) -lt $(($start_min)) ]; then
        start_min=$start
    fi

    end=`tail -n 2 $SSHCMD_LOG_PATH/$ip.log | head -n 1`
    if [ $(($end)) -gt $(($end_max)) ]; then
        end_max=$end
    fi

    interval=`tail -n 1 $SSHCMD_LOG_PATH/$ip.log`
    if [ $(($interval)) -gt $(($interval_max)) ]; then
        interval_max=$interval
        interval_max_ip=$ip
    fi

done < $IPLIST_PATH

echo "\n\nstart_min=$start_min"
echo "end_max=$end_max"
echo "interval_max=$interval_max ms, ip=$interval_max_ip"
echo "completed in $(($end_max-$start_min)) ms"
