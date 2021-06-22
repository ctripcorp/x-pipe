DIR=`dirname $0`

start_time=$(date +%s%3N)
nohup iptables -F $ip >> $DIR/nohup.out 2>&1 &

wait
#echo `ps -ef | grep isolate.sh | grep -v grep | wc -l`

end_time=$(date +%s%3N)

echo -e "$start_time\n$end_time\n$(($end_time-$start_time))" > $DIR/recovery_cron.log 2>&1 &

#cancel cron
echo "" > /var/spool/cron/xpipe
