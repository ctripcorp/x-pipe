echo $*
nohup /usr/local/bin/redis-server > /dev/null 2>&1 < /dev/null &
#make sure redis has started
sleep 1
ps -ef | grep "redis-serve[r]" | while read line; do
    echo $line
done