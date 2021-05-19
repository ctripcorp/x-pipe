DIR=`dirname $0`
echo $DIR

PORT=${DIR##*/}
CONFIG=$DIR/master.conf
DATA=$DIR/data
LOG=/opt/data/redis/$(($PORT / 1000))/$PORT
echo $LOG
SLEEP=1

mkdir -p $DATA
mkdir -p $LOG
chmod 755 $LOG

if [ -n "$PORT" ]; then
    echo "port from dir:$PORT"
    sed -i "s/port.*/port $PORT/" $DIR/*.conf
fi

PORT=$(cat $CONFIG | grep port | awk '{print $2}')

echo "Using config file:"$CONFIG $PORT

preCount=0

if [ $preCount -ge 0 ]; then
    sleep 1
fi

DATA_DIR=$DIR"/data"
sed -i "s#dir.*#dir $DATA_DIR#" $DIR/*.conf

REDIS=redis-server
if [ -f ~/redis/redis-server ]; then
    REDIS=~/redis/redis-server
fi

nohup $REDIS $CONFIG >>$LOG/master.log 2>&1 &
sleep $SLEEP
