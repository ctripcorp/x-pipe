#!/bin/bash
########### functions ###########
function getPortFromPath(){
    path=$1
    match=`echo $path | perl -ne "/_[0-9]{3,5}/ and print \"ok\""`
    if [ "$match" == "ok" ]; then
        port=`echo $path | perl -pe "s/.*?([0-9]{3,5}).*/\1/"`
    fi
    echo $port
}
function getPortFromPathOrDefault(){
    path=$1
    def=$2
    result=`getPortFromPath $path`
    if [ -z $result ];then
        result=$def
    fi
    echo $result
}
function toUpper(){
    echo $(echo $1 | tr [a-z] [A-Z])
}
function getEnv(){
    ENV=local
    if [ -f /opt/settings/server.properties ];then
        ENV=`cat /opt/settings/server.properties | egrep -i "^env" | awk -F= '{print $2}'`
    fi
    echo `toUpper $ENV`
}
function makedir(){
    if [ ! -d $1 ]; then
        echo "log dir not exist, create it"
        mkdir -p $1
    fi
}
function changeAndMakeLogDir(){
    current=$1
    logdir=$2
    makedir $logdir
    #../xx.conf
    sed -i 's#LOG_FOLDER=\(.*\)#LOG_FOLDER='"$logdir"'#'  $current/../*.conf
    sed -i 's#name="baseDir">.*</Property>#name="baseDir">'$logdir'</Property>#'   $current/../config/log4j2.xml
}
function changePort(){
    conf=$1
    port=$2
    if grep "server.port" $conf;then
        sed -i 's/server.port=[0-9]\+/server.port='$port'/' $conf
    else
        echo no port in path
        sed -i 's/JAVA_OPTS="/JAVA_OPTS="-Dserver.port='$port' /'  $conf
    fi
}
function getCurrentRealPath(){
    source="${BASH_SOURCE[0]}"
    while [ -h "$source" ]; do # resolve $source until the file is no longer a symlink
      dir="$( cd -P "$( dirname "$source" )" && pwd )"
      source="$(readlink "$source")"
      [[ $source != /* ]] && source="$dir/$source" # if $source was a relative symlink, we need to resolve it relative to the path where the symlink file was located
    done
    dir="$( cd -P "$( dirname "$source" )" && pwd )"
    echo $dir
}

function trySaveHeapTrace() {
    logdir=$1
    suffix=`date +%Y%m%d%H%M%S`
    if [[ -f "$logdir/heap_trace.txt" ]]; then
        cp "$logdir/heap_trace.txt" "$logdir/heap_trace_$suffix.txt"
    fi
}

function tryRemoveHeapTrace() {
    logdir=$1
    suffix=`date -d "$(date +%Y%m)01 last month" +%Y%m`
    find "$logdir" -type f -name "heap_trace_$suffix*.txt" -delete
}

#VARS
FULL_DIR=`getCurrentRealPath`
SERVICE_NAME=redis-meta
LOG_DIR=/opt/logs/100004375
`trySaveHeapTrace ${LOG_DIR}`
`tryRemoveHeapTrace ${LOG_DIR}`
SERVER_PORT=`getPortFromPathOrDefault $FULL_DIR 8080`
JMX_PORT=` expr $SERVER_PORT + 10000 `
IP=`ifconfig | grep "inet.10" | awk '{print $2}; NR == 1 {exit}'`

if [ ! $SERVER_PORT -eq 8080 ];then
    LOG_DIR=${LOG_DIR}_$SERVER_PORT
fi
echo port:$SERVER_PORT, jmx_port:$JMX_PORT, local ip:$IP
echo log:$LOG_DIR

#make sure log right
changeAndMakeLogDir $FULL_DIR $LOG_DIR
changePort $FULL_DIR/../$SERVICE_NAME.conf $SERVER_PORT

#get total memory
ENV=`getEnv`
echo "current env:"$ENV
if [ $ENV = "PRO" ]
then
    #GB
    USED_MEM=8
    XMN=6
    MAX_DIRECT=1
    META_SPACE=256
    MAX_META_SPACE=256
    JAVA_OPTS="$JAVA_OPTS -Xms${USED_MEM}g -Xmx${USED_MEM}g  -XX:+AlwaysPreTouch -Xmn${XMN}g -XX:MaxDirectMemorySize=${MAX_DIRECT}g -XX:MetaspaceSize=${META_SPACE}m -XX:MaxMetaspaceSize=${MAX_META_SPACE}m"
elif [ $ENV = "FWS" ] || [ $ENV = "FAT" ];then
    #MB
    USED_MEM=600
    XMN=450
    MAX_DIRECT=100
    META_SPACE=128
    MAX_META_SPACE=128
    JAVA_OPTS="$JAVA_OPTS -Xms${USED_MEM}m -Xmx${USED_MEM}m -Xmn${XMN}m -XX:+AlwaysPreTouch  -XX:MaxDirectMemorySize=${MAX_DIRECT}m -XX:MetaspaceSize=${META_SPACE}m -XX:MaxMetaspaceSize=${MAX_META_SPACE}m"
else
    #MB
    USED_MEM=800
    XMN=600
    MAX_DIRECT=100
    META_SPACE=128
    MAX_META_SPACE=128
    JAVA_OPTS="$JAVA_OPTS -Xms${USED_MEM}m -Xmx${USED_MEM}m -Xmn${XMN}m -XX:+AlwaysPreTouch  -XX:MaxDirectMemorySize=${MAX_DIRECT}m -XX:MetaspaceSize=${META_SPACE}m -XX:MaxMetaspaceSize=${MAX_META_SPACE}m"
fi
export JAVA_OPTS="$JAVA_OPTS -XX:+UseParNewGC -XX:MaxTenuringThreshold=5 -XX:+UseConcMarkSweepGC -XX:+UseCMSInitiatingOccupancyOnly -XX:+ScavengeBeforeFullGC -XX:+UseCMSCompactAtFullCollection -XX:+CMSParallelRemarkEnabled -XX:CMSFullGCsBeforeCompaction=9 -XX:CMSInitiatingOccupancyFraction=60 -XX:-CMSClassUnloadingEnabled -XX:SoftRefLRUPolicyMSPerMB=0 -XX:-ReduceInitialCardMarks -XX:+CMSPermGenSweepingEnabled -XX:CMSInitiatingPermOccupancyFraction=70 -XX:+ExplicitGCInvokesConcurrent -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationConcurrentTime -XX:+PrintHeapAtGC -XX:+HeapDumpOnOutOfMemoryError -XX:-OmitStackTraceInFastThrow -Duser.timezone=Asia/Shanghai -Dclient.encoding.override=UTF-8 -Dfile.encoding=UTF-8 -Xloggc:$LOG_DIR/heap_trace.txt -XX:HeapDumpPath=$LOG_DIR/HeapDumpOnOutOfMemoryError/  -Dcom.sun.management.jmxremote.port=$JMX_PORT -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=${IP} -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -Dio.netty.allocator.useCacheForAllThreads=false -Djava.security.egd=file:/dev/./urandom"

PATH_TO_JAR=$SERVICE_NAME".jar"
SERVER_URL="http://localhost:$SERVER_PORT"
STARTUP_LOG=$LOG_DIR"/startup.logger"

#set the jdk to 1.8 version
if [[ -z "$JAVA_HOME" && -d /usr/java/jdk1.8.0_121/ ]]; then
    export JAVA_HOME=/usr/java/jdk1.8.0_121
elif [[ -z "$JAVA_HOME" && -d /usr/java/latest/ ]]; then
    export JAVA_HOME=/usr/java/latest/
fi

cd `dirname $0`/..

for i in `ls $SERVICE_NAME-package-*.jar 2>/dev/null`
do
    if [[ ! $i == *"-sources.jar" ]]
    then
        PATH_TO_JAR=$i
        break
    fi
done

if [[ ! -f PATH_TO_JAR && -d current ]]; then
    cd current
    for i in `ls $SERVICE_NAME-package-*.jar 2>/dev/null`
    do
        if [[ ! $i == *"-sources.jar" ]]
        then
            PATH_TO_JAR=$i
            break
        fi
    done
fi

if [[ -f $SERVICE_NAME".jar" ]]; then
  rm -rf $SERVICE_NAME".jar"
fi

printf "$(date) ==== Starting ==== \n" > $STARTUP_LOG

ln $PATH_TO_JAR $SERVICE_NAME".jar"
chmod a+x $SERVICE_NAME".jar"
./$SERVICE_NAME".jar" start

rc=$?;

if [[ $rc != 0 ]];
then
    echo "$(date) Failed to start $SERVICE_NAME.jar, return code: $rc" >> $STARTUP_LOG
    exit $rc;
fi

declare -i counter=0
declare -i max_counter=16 # 16*5=80s
declare -i total_time=0

printf "Waiting for server startup" >> $STARTUP_LOG
until [[ (( counter -ge max_counter )) || "$(curl -X GET --silent --connect-timeout 1 --head $SERVER_URL | grep "Coyote")" != "" ]];
do
    printf "." >> $STARTUP_LOG
    counter+=1
    sleep 5
done

total_time=counter*5

if [[ (( counter -ge max_counter )) ]];
then
    printf "\n$(date) Server failed to start in $total_time seconds!\n" >> $STARTUP_LOG
    exit 1;
fi

printf "\n$(date) Server started in $total_time seconds!\n" >> $STARTUP_LOG

exit 0;
