DIR=$(dirname $0)
count=$1

echo "sentinel count:"$count
startPort=$2

function startSentinel() {

    currentPort=$1
    sleep=$2
    if [ -z $sleep ]; then
        sleep=1
    fi
    echo ========start sentinel $currentPort=================

    rm -rf $DIR/$currentPort
    mkdir -p $DIR/$currentPort
    cp $DIR/template/sentinel.sh $DIR/template/sentinel.conf $DIR/$currentPort

    sh $DIR/$currentPort/sentinel.sh
    echo "start sentinel: $DIR/$currentPort"
}

for ((i = 0; i < $count; i++)); do
    port=$(($startPort + $i))
    startSentinel $port 0
done
