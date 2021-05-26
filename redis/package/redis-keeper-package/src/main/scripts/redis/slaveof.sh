count=$1
start_port=$2
ip=`ifconfig | grep broadcast | awk '{print $2}'`

for ((i = 0; i < $count; i++)); do
    port=$(($start_port + $i))

    REDIS_CLI=redis-cli
    if [ -f ~/redis/redis-cli ]; then
        REDIS_CLI=~/redis/redis-cli
    fi

    if [ $(($port % 2)) -eq 0 ]; then
        $REDIS_CLI -h $ip -p $port slaveof no one
    else
        $REDIS_CLI -h $ip -p $port slaveof $ip $(($port - 1))
        echo "make $port slaveof $(($port - 1))"
    fi

done
