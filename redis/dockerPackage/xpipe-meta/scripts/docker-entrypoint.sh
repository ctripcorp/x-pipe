#!/bin/bash

#vars
LOCAL_PATH=config
SERVICE_NAME=meta
XPIPE_CONF=xpipe

XPIPE_ROOT_DIR=/xpipe-meta
LOG_DIR=/opt/logs/100004375


#functions
function changeDataCenter(){
    newDc=$1
    sed -i  "s/\(datacenter=\)jq/\1$newDc/g" $LOCAL_PATH/$XPIPE_CONF.properties
    sed -i  "s/\(metaserver-local-\)jq/\1$newDc/g" $LOCAL_PATH/$XPIPE_CONF.properties
}

function makedir(){
    if [ ! -d $1 ]; then
        echo "log dir not exist, create it"
        mkdir -p $1
    fi
}

makedir $LOG_DIR

cd $XPIPE_ROOT_DIR

if [ $CONSOLE_ADDRESS ];then
    sed -i "s#http://consolejq:8080#$CONSOLE_ADDRESS#g" $LOCAL_PATH/$XPIPE_CONF.properties
fi

if [ $DATACENTER ];then
    changeDataCenter $DATACENTER
fi

if [ $METASERVER_ID ];then
    sed -i  "s/\(metaserver.id=\)1/\1$METASERVER_ID/g" $LOCAL_PATH/$XPIPE_CONF.properties
fi

if [ $ZKADDRESS ];then
    sed -i  "s#zoo1:2181#$ZKADDRESS#g" $LOCAL_PATH/$XPIPE_CONF.properties
fi

chmod a+x $SERVICE_NAME".jar"
./$SERVICE_NAME.jar start