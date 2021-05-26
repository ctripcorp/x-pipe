DIR=`dirname $0`
num=$1
start_port=$2
thread_num=40

echo "redis batch start"

port=$start_port
for ((i = 0; i < $thread_num; i++)); do
    if [ $(($num % $thread_num)) -gt $i ]; then
        compensate=1
    else
        compensate=0
    fi
    count=$(($num / $thread_num + $compensate))
    nohup $DIR/start.sh $count $port >$DIR/$(($port / 1000)).log 2>&1 &
    port=$(($port + $count))
done
echo "redis batch start end"
