count=$1
port=$2
slavecount=1

echo "redis count:"$count

DIR=`dirname $0`
function startRedis() {

    currentPort=$1
    sleep=$2
    if [ -z $sleep ]; then
        sleep=1
    fi
    echo ========start redis $currentPort=================

    slice=$(($currentPort / 1000))
    if [ ! -d $DIR/$slice ]; then
        mkdir $DIR/$slice
    fi
    rm -rf $DIR/$slice/$currentPort
    mkdir -p $DIR/$slice/$currentPort
    cp $DIR/template/redis.sh $DIR/template/master.conf $DIR/$slice/$currentPort

    sh $DIR/$slice/$currentPort/redis.sh
    echo "start redis: $slice/$currentPort"
}

for ((i = 0; i < $count; i++)); do
    startRedis $(($port + $i)) 0
done
