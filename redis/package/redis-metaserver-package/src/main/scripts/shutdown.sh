#!/bin/bash
SERVICE_NAME=redis-meta

#set the jdk to 1.8 version
if [[ -z "$JAVA_HOME" && -d /usr/java/jdk1.8/ ]]; then
    export JAVA_HOME=/usr/java/jdk1.8
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
