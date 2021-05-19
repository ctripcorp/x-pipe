DIR=`dirname $0`
CONFIG_FILE=~/redis/config
dc_type=$1

if [ "active" == "$dc_type" ]; then
    echo "start active idc"
elif [ "backup" == "$dc_type" ]; then
    echo "start backup idc"
else
    echo "Input error. DC_TYPE param must be 'active' or 'backup'"
    exit
fi

mkdir -p /opt/data/redis
mkdir -p /opt/data/sentinel

#start sentinel
sentinel_num=$(grep sentinel_num $CONFIG_FILE | awk -F = '{print $2}')
sentinel_start_port=$(grep sentinel_start_port $CONFIG_FILE | awk -F = '{print $2}')

echo "sentinel stop start"
$DIR/sentinel/stop.sh
echo "sentinel batch start"
nohup $DIR/sentinel/start.sh $sentinel_num $sentinel_start_port >$DIR/sentinel/5.log 2>&1 &

#start redis
redis_num=$(grep redis_num $CONFIG_FILE | awk -F = '{print $2}')
redis_start_port=$(grep redis_start_port $CONFIG_FILE | awk -F = '{print $2}')

echo "redis stop start"
$DIR/redis/stop.sh
echo "redis batch start"
nohup $DIR/redis/redis_batch.sh $redis_num $redis_start_port >$DIR/redis/redis_batch.log 2>&1 &

if [ "active" == "$dc_type" ]; then
    while [ $redis_num -gt $((`ps -ef | grep redis-server | grep -v grep | wc -l`)) ]; do
        echo "redis started num:`ps -ef | grep redis-server | grep -v grep | wc -l`/$redis_num"
        sleep 1
    done
    #slaveof
    echo "slaveof batch start"
    nohup $DIR/redis/slaveof_batch.sh $redis_num $redis_start_port >$DIR/redis/slave_batch.log 2>&1 &
    echo "slaveof batch end"
fi
