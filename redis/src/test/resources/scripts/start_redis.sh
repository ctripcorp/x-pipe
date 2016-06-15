echo $*
nohup /usr/local/bin/redis-server $1 > $2 2>&1 &