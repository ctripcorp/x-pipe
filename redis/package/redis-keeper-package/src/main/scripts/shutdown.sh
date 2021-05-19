#!/bin/bash
SERVICE_NAME=redis-keeper

#set the jdk to 1.8 version
if [[ -z "$JAVA_HOME" && -d /usr/java/jdk1.8.0_121/ ]]; then
    export JAVA_HOME=/usr/java/jdk1.8.0_121
elif [[ -z "$JAVA_HOME" && -d /usr/java/latest/ ]]; then
    export JAVA_HOME=/usr/java/latest/
fi

cd `dirname $0`/..

if [[ ! -f $SERVICE_NAME".jar" && -d current ]]; then
    cd current
fi

if [[ -f $SERVICE_NAME".jar" ]]; then
  chmod a+x $SERVICE_NAME".jar"
  ./$SERVICE_NAME".jar" stop
fi

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

ENV=`getEnv`
echo "current env:"$ENV
if [ $ENV = "UAT" ]
then
    DIR=`dirname $0`
    CURRENT_SCRIPT_PATH="$DIR/../current/scripts"

    $CURRENT_SCRIPT_PATH/stop_all.sh
fi