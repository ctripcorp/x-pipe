echo $*
ps -ef | grep 'redi[s]-server' | awk '{print $2}' | xargs kill -9
