DIR=`dirname $0`
num=$1
start_port=$2
thread_num=40

echo "slaveof start"
port=$start_port
for ((i = 0; i < $thread_num; i++)); do
    if [ $(($num % $thread_num)) -gt $i ]; then
        compensate=1
    else
        compensate=0
    fi
    count=$(($num / $thread_num + $compensate))
    echo "count=$count, port=$port"
    nohup $DIR/slaveof.sh $count $port >$DIR/s$(($port / 1000)).log 2>&1 &
    port=$(($port + $count))
done

echo "slaveof end"
