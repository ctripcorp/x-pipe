ps -ef | grep redis-server | grep -v grep | awk '{print $2}' | xargs kill -9
ps -ef | grep redis-sentinel | grep -v grep | awk '{print $2}' | xargs kill -9
