DIR=$(dirname $0)
echo $DIR

PORT=${DIR##*/}
CONFIG=$DIR/sentinel.conf
DATA=$DIR/data
LOG=/opt/data/sentinel/$PORT
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

DATA_DIR=$DIR"/data"
sed -i "s#dir.*#dir $DATA_DIR#" $DIR/*.conf

REDIS=redis-sentinel
if [ -f ~/redis/redis-sentinel ]; then
    REDIS=~/redis/redis-sentinel
fi

nohup $REDIS $CONFIG >>$LOG/sentinel.log 2>&1 &
chmod 644 $LOG/sentinel.log

sleep $SLEEP
