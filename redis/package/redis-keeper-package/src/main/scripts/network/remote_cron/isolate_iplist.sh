DIR=`dirname $0`
local_ip=`ifconfig | grep broadcast | awk '{print $2}'`
iplist_file=$local_ip

start_time=$(date +%s%3N)
while read ip
do
    nohup $DIR/isolate.sh $ip > $DIR/nohup.out 2>&1 &
done < $DIR/$iplist_file

wait

end_time=$(date +%s%3N)

echo -e "$start_time\n$end_time\n$(($end_time-$start_time))" > $DIR/isolate_cron.log 2>&1 &
