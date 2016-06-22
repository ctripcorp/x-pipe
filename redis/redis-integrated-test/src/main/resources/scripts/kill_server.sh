echo $*
lsof -i:$1 -nP | grep LISTEN | awk 'NR!=1{print $0}' | while read line; do
    pid=`echo $line | awk '{print  $2}'`
    echo killing $line
    kill -9 $pid
done