ps -ef | grep redis-sentinel | grep -v grep | awk '{print $2}' | xargs kill -9
