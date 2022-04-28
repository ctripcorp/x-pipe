DIR=`dirname $0`
REMOTE_SCRIPT_DIR=$DIR/remote_cron
UPLOAD_SCRIPT_DIR=$DIR./upload/

dc=$1
isolate_hour=$2
isolate_minute=$3
beacon=$4
upload_sh=$5

if [ -z $dc ]; then
    echo "Input Error. DC cannot be null"
    exit
fi

if [ -z $isolate_hour ]; then
    echo "Input Error. Hour cannot be null"
    exit
fi

if [ -z $isolate_minute ]; then
    echo "Input Error. Minute cannot be null"
    exit
fi

#kill all sshpass process before start
ps -ef | grep sshpass | awk '{print $2,$8}' | grep -v grep | awk '{print $1}' | xargs kill -9

#generate iplists
if [ "$beacon" == "beacon" ]; then
    echo "mode: just isolate beacon"
    $DIR/generate_iplist_just_isolate_beacon.sh $dc
else
    echo "mode: isolate whole DC and just core path accpet"
    $DIR/generate_iplist_just_core_path_accept.sh $dc
fi
#check time
echo "============================start check time==============================="
$DIR/check_time.sh $DIR/iplist

#change add_cron.sh
echo "sudo sh -c 'echo \"$isolate_minute $isolate_hour * * * sudo ~/remote_cron/accept_core_path_iplist.sh\" > /var/spool/cron/xpipe'" > $REMOTE_SCRIPT_DIR/add_cron.sh
chmod +x $REMOTE_SCRIPT_DIR/add_cron.sh

#upload remote_cron folder
#only need upload first time
echo "============================start upload scripts==============================="
remote_dir="~"
if [ "$upload_sh" == "n" ]; then
    echo "upload add_cron.sh only"
    $UPLOAD_SCRIPT_DIR/batch_upload_files.sh $REMOTE_SCRIPT_DIR/add_cron.sh $remote_dir/remote_cron noremove $DIR/iplist
else
    echo "upload remote_cron folder"
    $UPLOAD_SCRIPT_DIR/batch_upload_files.sh $REMOTE_SCRIPT_DIR $remote_dir noremove $DIR/iplist
fi

#upload iplist
echo "============================start upload iplists==============================="
$DIR/upload_iplists.sh

#ssh trigger add cron
echo "============================start trigger cron==============================="
$DIR/trigger_add_cron.sh