ps -ef | grep redis-server | grep -v grep | awk '{print $2}' | xargs kill -9
