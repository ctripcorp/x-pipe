DIR=`dirname $0`
echo $DIR

PORT=${DIR##*/}
CONFIG=$DIR/master.conf
DATA=$DIR/data
FILE=/opt/data/redis/$(($PORT / 1000))/$PORT
echo $FILE
LOG=$FILE
SLEEP=1

mkdir -p $DATA
mkdir -p $FILE
chmod 755 $FILE

function toUpper(){
    echo $(echo $1 | tr [a-z] [A-Z])
}

function getType(){
    TYPE=NORMAL
    if [ -f /opt/settings/server.properties ];then
        TYPE=`cat /opt/settings/server.properties | egrep -i "^type" | awk -F= '{print $2}'`
    fi
    echo `toUpper $TYPE`
}

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

TYPE=`getType`
echo "TYPE:"$TYPE
if [ $TYPE = "LARGE" ];then
    DATA_DIR="$FILE/data"
    mkdir -p $DATA_DIR
    chmod 755 $DATA_DIR
    cp /opt/data/redis-rdb/dump.rdb $DATA_DIR
    sed -i "s#dir.*#dir $DATA_DIR#" $DIR/*.conf
    sed -i 's/^maxmemory.*mb$/maxmemory 10gb/' $DIR/*.conf
else
    DATA_DIR=$DIR"/data"
    sed -i "s#dir.*#dir $DATA_DIR#" $DIR/*.conf
fi

REDIS=redis-server
if [ -f ~/redis/redis-server ]; then
    REDIS=~/redis/redis-server
fi

nohup $REDIS $CONFIG >>$LOG/master.log 2>&1 &
sleep $SLEEP
