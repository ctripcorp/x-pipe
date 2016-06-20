echo $*
ps -ef | grep 'redi[s]-' | awk '{print $2}' | xargs kill -9