echo $*
lsof -i4tcp:$1 -nP | grep LISTEN | awk '{print $0}' | while read line; do
    pid=`echo $line | awk '{print  $2}'`
    echo killing $line
    kill -9 $pid
done


sleep 1
result=`lsof -i:$1 -nP | grep LISTEN | wc -l`
#if not killed all, exit with not 0
exit $result