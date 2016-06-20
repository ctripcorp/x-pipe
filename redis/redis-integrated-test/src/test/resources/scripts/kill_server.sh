echo $*
lsof -i:$1 -nP | awk 'NR!=1{print $2}' | xargs kill -9