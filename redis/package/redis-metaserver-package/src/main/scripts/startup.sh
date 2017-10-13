#!/bin/bash
SERVICE_NAME=redis-meta
LOG_DIR=/opt/logs/100004375
SERVER_PORT=8080
IP=`ifconfig | grep "inet.10" | awk '{print $2}; NR == 1 {exit}'`


########### vm args ###########
function toUpper(){
    echo $(echo $1 | tr [a-z] [A-Z])
}

#get total memory
ENV=`cat /opt/settings/server.properties | egrep -i "^env" | awk -F= '{print $2}'`
ENV=`toUpper $ENV`
echo "current env:"$ENV
if [ $ENV = "PRO" ]
then
    #GB
    USED_MEM=8
    XMN=6
    MAX_DIRECT=1
    JAVA_OPTS="$JAVA_OPTS -Xms${USED_MEM}g -Xmx${USED_MEM}g  -XX:+AlwaysPreTouch -Xmn${XMN}g -XX:MaxDirectMemorySize=${MAX_DIRECT}g"
else
    #MB
    USED_MEM=800
    XMN=600
    MAX_DIRECT=100
    JAVA_OPTS="$JAVA_OPTS -Xms${USED_MEM}m -Xmx${USED_MEM}m -Xmn${XMN}m -XX:+AlwaysPreTouch  -XX:MaxDirectMemorySize=${MAX_DIRECT}m"
fi
export JAVA_OPTS="$JAVA_OPTS -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=128m  -XX:+UseParNewGC -XX:MaxTenuringThreshold=5 -XX:+UseConcMarkSweepGC -XX:+UseCMSInitiatingOccupancyOnly -XX:+ScavengeBeforeFullGC -XX:+UseCMSCompactAtFullCollection -XX:+CMSParallelRemarkEnabled -XX:CMSFullGCsBeforeCompaction=9 -XX:CMSInitiatingOccupancyFraction=60 -XX:+CMSClassUnloadingEnabled -XX:SoftRefLRUPolicyMSPerMB=0 -XX:-ReduceInitialCardMarks -XX:+CMSPermGenSweepingEnabled -XX:CMSInitiatingPermOccupancyFraction=70 -XX:+ExplicitGCInvokesConcurrent -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationConcurrentTime -XX:+PrintHeapAtGC -XX:+HeapDumpOnOutOfMemoryError -XX:-OmitStackTraceInFastThrow -Duser.timezone=Asia/Shanghai -Dclient.encoding.override=UTF-8 -Dfile.encoding=UTF-8 -Xloggc:$LOG_DIR/heap_trace.txt -XX:HeapDumpPath=$LOG_DIR/HeapDumpOnOutOfMemoryError/  -Dcom.sun.management.jmxremote.port=8301 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=${IP} -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -Djava.security.egd=file:/dev/./urandom"

PATH_TO_JAR=$SERVICE_NAME".jar"
SERVER_URL="http://localhost:$SERVER_PORT"
STARTUP_LOG=$LOG_DIR"/startup.logger"

if [[ -z "$JAVA_HOME" && -d /usr/java/latest/ ]]; then
    export JAVA_HOME=/usr/java/latest/
fi

cd `dirname $0`/..

for i in `ls package-$SERVICE_NAME-*.jar 2>/dev/null`
do
    if [[ ! $i == *"-sources.jar" ]]
    then
        PATH_TO_JAR=$i
        break
    fi
done

if [[ ! -f PATH_TO_JAR && -d current ]]; then
    cd current
    for i in `ls package-$SERVICE_NAME-*.jar 2>/dev/null`
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
