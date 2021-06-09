DIR=`dirname $0`
IPLIST_PATH="$DIR/iplists"
LOGPATH="$DIR/logs/uploadiplist"

mkdir -p $LOGPATH

uploading_iplist=()
iplist=( `ls $IPLIST_PATH` )
for ip in ${iplist[@]};
do
    uploading_iplist+=($ip)
done

remote_path=remote_cron
remove=no

while [ ${#uploading_iplist[@]} -gt 0 ]; do

    for ip in ${uploading_iplist[@]}; do
        nohup $DIR./upload/upload_files.sh $ip $IPLIST_PATH/$ip $remote_path $remove > $LOGPATH/$ip.log 2>&1 &
    done

    count=0
    while [ $((`ps -ef | grep upload_files.sh | grep -v batch_upload_files.sh | grep -v grep | wc -l`)) -gt 0 ]; do
        if [ $(($count%3)) -eq 1 ]; then
            echo "waiting ips:"
            echo `ps -ef | grep sshpass | grep -v grep | awk '{print $13}' | cut -b 7-`
        fi
        count=$(($count+1))
        sleep 1
    done

    retry_ip=()
    for src_ip in ${uploading_iplist[@]};
    do
        if [ $((`cat "$LOGPATH/$src_ip.log" | grep -v UTF | grep -v upload | grep -v echo | wc -l`)) -ne 0 ]; then
            retry_ip+=($src_ip)

            echo "Error. $src_ip uploaded failed. Due to:"
            echo `cat "$LOGPATH/$src_ip.log"`
        fi
    done
    
    echo "retrylist=${retry_ip[*]}"
    uploading_iplist=(${retry_ip[*]})
done

