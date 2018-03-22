echo $*
nohup /usr/local/bin/redis-server $1 > $2 2>&1 &
#make sure redis has started
sleep 1
ps -ef | grep "redis-serve[r]" | while read line; do
    echo $line
done