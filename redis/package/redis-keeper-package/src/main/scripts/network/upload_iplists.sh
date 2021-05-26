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

    while [ $((`ps -ef | grep upload_files.sh | grep -v batch_upload_files.sh | grep -v grep | wc -l`)) -gt 0 ]; do
        echo "uploading iplist....."
        sleep 1
    done
    echo "done!"

    retry_ip=()
    for src_ip in ${uploading_iplist[@]};
    do
        if [ $((`cat "$LOGPATH/$src_ip.log" | grep -v UTF | grep -v upload | grep -v echo | wc -l`)) -eq 0 ]; then
            echo "$src_ip upload iplist success"
        else
            retry_ip+=($src_ip)

            echo "Error. $src_ip uploaded failed. Due to:"
            echo `cat "$LOGPATH/$src_ip.log"`
        fi
    done
    
    echo "retrylist=${retry_ip[*]}"
    uploading_iplist=(${retry_ip[*]})
done

