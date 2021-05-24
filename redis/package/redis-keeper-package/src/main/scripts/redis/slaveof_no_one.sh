count=$1
start_port=$2
ip=`ifconfig | grep broadcast | awk '{print $2}'`

for ((i = 0; i < $count; i++)); do
    port=$(($start_port + $i))

    REDIS_CLI=redis-cli
    if [ -f ~/redis/redis-cli ]; then
        REDIS_CLI=~/redis/redis-cli
    fi
    $REDIS_CLI -h $ip -p $port slaveof no one
    echo "$port slaveof no one"
done
