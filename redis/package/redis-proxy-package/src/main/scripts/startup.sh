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

function getTotalMem() {
    echo `free -m | egrep "^Mem" | awk -F " " '{print $2}'`
}
function getSafeXmx() {
    total=`getTotalMem`
    SAFE_PERCENT=55
    MAX_MEM=7168

    if [ "$total" -gt 10240 ]
    then
      echo "$MAX_MEM"
    else
      echo `expr $total \* $SAFE_PERCENT / 100`
    fi
}

function getSafeXmn() {
    xmx=$1
    XMN_PERCENT=80
    echo `expr $xmx \* $XMN_PERCENT / 100`
}

function getSafeMaxDirect() {
    total=`getTotalMem`
    SAFE_PERCENT=10
    if [ "$total" -gt 10240 ]
    then
      echo 3072
    else
      echo `expr $total \* $SAFE_PERCENT / 100`
    fi
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

function tryRemoveJarLog() {
    logdir=$1
    appname=$2
    find "$logdir" -type f -name "$appname_*.log" -delete
}

#VARS
FULL_DIR=`getCurrentRealPath`
SERVICE_NAME=redis-proxy
SERVER_PORT=`getPortFromPathOrDefault $FULL_DIR 8080`
JMX_PORT=` expr $SERVER_PORT + 10000 `
IP=`ifconfig | grep "inet.10" | awk '{print $2}; NR == 1 {exit}'`
LOG_DIR=/opt/logs/100013684
`trySaveHeapTrace ${LOG_DIR}`
`tryRemoveHeapTrace ${LOG_DIR}`
`tryRemoveJarLog ${LOG_DIR} ${SERVICE_NAME}`

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
  #MB
  USED_MEM=`getSafeXmx`
  XMN=`getSafeXmn $USED_MEM`
  MAX_DIRECT=`getSafeMaxDirect`
  JAVA_OPTS="$JAVA_OPTS -Xms${USED_MEM}m -Xmx${USED_MEM}m -Xmn${XMN}m -XX:+AlwaysPreTouch  -XX:MaxDirectMemorySize=${MAX_DIRECT}m"
export JAVA_OPTS="-server $JAVA_OPTS -XX:+UnlockExperimentalVMOptions -XX:+UseZGC -XX:MaxTenuringThreshold=1 --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true -Dio.netty.maxDirectMemory=-1 -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=128m -XX:+HeapDumpOnOutOfMemoryError -XX:-OmitStackTraceInFastThrow -Duser.timezone=Asia/Shanghai -Dlog4j2.asyncLoggerRingBufferSize=32768 -Dclient.encoding.override=UTF-8 -Dfile.encoding=UTF-8 -Xlog:safepoint,classhisto*=trace,age*,gc*=info:file=$LOG_DIR/gc-%t.log:time,tid,tags:filecount=5,filesize=50m -XX:HeapDumpPath=$LOG_DIR/HeapDumpOnOutOfMemoryError/  -Dcom.sun.management.jmxremote.port=$JMX_PORT -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=${IP} -XX:+FlightRecorder -Djava.security.egd=file:/dev/./urandom"

export LD_PRELOAD=/usr/local/lib/libjemalloc.so
export MALLOC_CONF=prof:false,lg_prof_interval:30,lg_prof_sample:20,prof_prefix:/opt/logs/100013684/jeprof

PATH_TO_JAR=$SERVICE_NAME".jar"
SERVER_URL="http://localhost:$SERVER_PORT"
STARTUP_LOG=$LOG_DIR"/startup.logger"

ARCH=`uname -r`
#set the jdk to 11/17 version
if [[ -z "$JAVA_HOME" && -d /usr/java/jdk21/ ]]; then
    export JAVA_HOME=/usr/java/jdk21
    JAVA_OPTS="$JAVA_OPTS -XX:+ZGenerational"
elif [[ -z "$JAVA_HOME" && -d /usr/java/jdk11/ ]]; then
    export JAVA_HOME=/usr/java/jdk11
elif [[ -z "$JAVA_HOME" && -d /usr/java/latest/ ]]; then
    export JAVA_HOME=/usr/java/latest/
fi

echo $JAVA_OPTS

cd `dirname $0`/..

for i in `ls $SERVICE_NAME-*.jar 2>/dev/null`
do
    if [[ ! $i == *"-sources.jar" ]]
    then
        PATH_TO_JAR=$i
        break
    fi
done

if [[ ! -f PATH_TO_JAR && -d current ]]; then
    cd current
    for i in `ls $SERVICE_NAME-*.jar 2>/dev/null`
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
until [[ (( counter -ge max_counter )) || "$(curl -X GET --silent --connect-timeout 1 --head $SERVER_URL/health | grep "HTTP/1.1 200")" != "" ]];
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
