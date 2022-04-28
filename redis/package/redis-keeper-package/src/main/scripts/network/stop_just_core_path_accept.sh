DIR=`dirname $0`
REMOTE_SCRIPT_DIR=$DIR/remote_cron
UPLOAD_SCRIPT_DIR=$DIR./upload/

recovery_hour=$1
recovery_minute=$2

IPLIST_PATH="$DIR/iplist"

if [ -z $recovery_hour ]; then
    echo "Input Error. Hour cannot be null"
    exit
fi

if [ -z $recovery_minute ]; then
    echo "Input Error. Minute cannot be null"
    exit
fi

#kill all sshpass process before start
ps -ef | grep sshpass | awk '{print $2,$8}' | grep -v grep | xargs kill -9

#change add_cron.sh
echo "sudo sh -c 'echo \"$recovery_minute $recovery_hour * * * sudo ~/remote_cron/recovery_core_path.sh\" > /var/spool/cron/xpipe'" > $REMOTE_SCRIPT_DIR/add_cron.sh
chmod +x $REMOTE_SCRIPT_DIR/add_cron.sh

#upload remote_cron folder
#only need upload first time
echo "============================start upload scripts==============================="
remote_dir="~"
    $UPLOAD_SCRIPT_DIR/batch_upload_files.sh $REMOTE_SCRIPT_DIR/add_cron.sh $remote_dir/remote_cron noremove $DIR/iplist

#ssh trigger add cron
echo "============================start trigger cron==============================="
$DIR/trigger_add_cron.sh