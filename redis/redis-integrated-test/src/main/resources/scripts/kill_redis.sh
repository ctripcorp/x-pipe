echo $*
ps -ef | grep 'redi[s]-server' | awk '{print $2}' | xargs kill -9
sleep 1
exit `ps -ef | grep 'redi[s]-server' | wc -l`
