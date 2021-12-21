#!/bin/bash

#vars
SERVICE_NAME=console
XPIPE_CONF=xpipe
XPIPE_ROOT_DIR=/xpipe-console
LOG_DIR=/opt/logs/100004374
LOCAL_PATH=config

#functions
function changeDataCenter(){
    newDc=$1
    oldDc=$2
    sed -i  "s/\(datacenter = \)jq/\1$newDc/g" $LOCAL_PATH/$XPIPE_CONF.properties
    sed -i  "s/\(console-local-\)jq/\1$newDc/g" $LOCAL_PATH/$XPIPE_CONF.properties
    sed -i  "s#/$oldDc#$newDc#g" $LOCAL_PATH/$XPIPE_CONF.properties
}

function makedir(){
    if [ ! -d $1 ]; then
        echo "log dir not exist, create it"
        mkdir -p $1
    fi
}

makedir $LOG_DIR

cd $XPIPE_ROOT_DIR

if [ $DATACENTEROY ];then
    changeDataCenter $DATACENTEROY oy
fi

if [ $ZKADDRESS ];then
    sed -i  "s#zoo1.2181#$ZKADDRESS#g" $LOCAL_PATH/$XPIPE_CONF.properties
fi

if [ $ROLE == "console" ];then
  tee -a $LOCAL_PATH/$XPIPE_CONF.properties << EOF
console.server.mode = CONSOLE
console.cluster.divide.parts = 3
EOF
elif [ $ROLE == "checker" ]; then
  tee -a $LOCAL_PATH/$XPIPE_CONF.properties << EOF
console.server.mode = CHECKER
checker.clusters.part.index = 0
console.address = http://consolejq:8080
checker.address.all = http://checkerjq1:8080,http://checkerjq2:8080,http://checkerjq3:8080
EOF
fi

if [ $CHECKER_NUMS ]; then
  sed -i  "s/\(console.cluster.divide.parts = \)3/\1$CHECKER_NUMS/g" $LOCAL_PATH/$XPIPE_CONF.properties
fi

if [ $CHECKER_INDEX ];then
  sed -i  "s/\(checker.clusters.part.index = \)0/\1$CHECKER_INDEX/g" $LOCAL_PATH/$XPIPE_CONF.properties
fi

if [ $CONSOLE_ADDRESS ];then
  sed -i  "s/\(console.address = \)http://consolejq:8080/\1$CONSOLE_ADDRESS/g" $LOCAL_PATH/$XPIPE_CONF.properties
fi

if [ $CONSOLE_QUORUM ];then
  sed -i  "s/\(console.quorum = \)1/\1$CONSOLE_QUORUM/g" $LOCAL_PATH/$XPIPE_CONF.properties
fi

if [ $CHECKER_ADDRESS_ALL ];then
  sed -i  "s#http://checkerjq1:8080,http://checkerjq2:8080,http://checkerjq3:8080#$CHECKER_ADDRESS_ALL#g" $LOCAL_PATH/$XPIPE_CONF.properties
fi

chmod a+x $SERVICE_NAME".jar"

./$SERVICE_NAME.jar start