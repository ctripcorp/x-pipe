#!/usr/bin/env bash
TIMEOUT_START_SEC=600

function toUpper() {
    echo $(echo $1 | tr [a-z] [A-Z])
}
function getEnv() {
    ENV=local
    if [ -f /opt/settings/server.properties ]; then
        ENV=$(cat /opt/settings/server.properties | egrep -i "^env" | awk -F= '{print $2}')
    fi
    echo $(toUpper $ENV)
}

ENV=$(getEnv)
echo "current env:"$ENV
if [ $ENV = "UAT" ]; then
    #add logrotate
    LOGROTATE_FILE_REDIS=/etc/logrotate.d/redis
    echo "/opt/data/redis/*/*/master.log {" >$LOGROTATE_FILE_REDIS
    echo "    daily" >>$LOGROTATE_FILE_REDIS
    echo "    rotate 30" >>$LOGROTATE_FILE_REDIS
    echo "    missingok" >>$LOGROTATE_FILE_REDIS
    echo "    dateext" >>$LOGROTATE_FILE_REDIS
    echo "    copytruncate" >>$LOGROTATE_FILE_REDIS
    echo "}" >>$LOGROTATE_FILE_REDIS

    LOGROTATE_FILE_SENTINEL=/etc/logrotate.d/sentinel
    echo "/opt/data/sentinel/*/sentinel.log {" >$LOGROTATE_FILE_SENTINEL
    echo "    daily" >>$LOGROTATE_FILE_SENTINEL
    echo "    rotate 30" >>$LOGROTATE_FILE_SENTINEL
    echo "    missingok" >>$LOGROTATE_FILE_SENTINEL
    echo "    dateext" >>$LOGROTATE_FILE_SENTINEL
    echo "    copytruncate" >>$LOGROTATE_FILE_SENTINEL
    echo "}" >>$LOGROTATE_FILE_SENTINEL

    #change systemd config
    cd `dirname $0`
    CURRENT_DIR=`ls -l ../ | grep current`
    if [ -z "$CURRENT_DIR" ]; then
        APPID=`cat ../WEB-INF/classes/META-INF/app.properties | sed -r "s/app.id=([0-9]+)/\1/g" | tr -d '\\r'`
    else
        APPID=`cat ../current/WEB-INF/classes/META-INF/app.properties | sed -r "s/app.id=([0-9]+)/\1/g" | tr -d '\\r'`
    fi

    LOG_DIR=/opt/logs/$APPID
    BEFORE_SHUTDOWN_LOG_FILE=$LOG_DIR/before_shutdown.log
    touch $BEFORE_SHUTDOWN_LOG_FILE
    echo "" >>$BEFORE_SHUTDOWN_LOG_FILE
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ======= START sudo_before_shutdown.sh" >>$BEFORE_SHUTDOWN_LOG_FILE

    SERVICE_FILE=/usr/lib/systemd/system/ctripapp@$APPID.service
    if [ ! -f "$SERVICE_FILE" ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ======= service file not found: $SERVICE_FILE" >>$BEFORE_SHUTDOWN_LOG_FILE
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ======= END sudo_before_shutdown.sh" >>$BEFORE_SHUTDOWN_LOG_FILE
        exit 0
    fi

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ======= service file found: $SERVICE_FILE" >>$BEFORE_SHUTDOWN_LOG_FILE
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ======= service file content before:" >>$BEFORE_SHUTDOWN_LOG_FILE
    cat $SERVICE_FILE >>$BEFORE_SHUTDOWN_LOG_FILE
    echo "" >>$BEFORE_SHUTDOWN_LOG_FILE
    sed -i "/TimeoutStartSec=/d" $SERVICE_FILE
    sed -i "/\[Service\]/a\TimeoutStartSec=$TIMEOUT_START_SEC" $SERVICE_FILE
    systemctl enable $SERVICE_FILE
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ======= service file content after:" >>$BEFORE_SHUTDOWN_LOG_FILE
    cat $SERVICE_FILE >>$BEFORE_SHUTDOWN_LOG_FILE
    echo "" >>$BEFORE_SHUTDOWN_LOG_FILE
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ======= END sudo_before_shutdown.sh" >>$BEFORE_SHUTDOWN_LOG_FILE
fi

exit 0
