#!/bin/bash

#vars
SERVICE_NAME=proxy
XPIPE_CONF=xpipe
LOG_DIR=/opt/logs/100013684/
XPIPE_ROOT_DIR=/xpipe-proxy

function makedir(){
    if [ ! -d $1 ]; then
        echo "log dir not exist, create it"
        mkdir -p $1
    fi
}

makedir $LOG_DIR

cd $XPIPE_ROOT_DIR
chmod a+x $SERVICE_NAME".jar"
./$SERVICE_NAME.jar start

