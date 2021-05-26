DIR=`dirname $0`
LOGPATH="$DIR/logs"
IPLIST="$DIR/upload_iplist"

local_path=$1
remote_path="$2"
remove=$3
iplist=$4

if [ -z $iplist ]; then
    iplist=$IPLIST
fi

mkdir -p $LOGPATH

uploading_iplist=()
while read ip
do
    uploading_iplist+=($ip)
done < $iplist

while [ ${#uploading_iplist[@]} -gt 0 ]; do

    for ip in ${uploading_iplist[@]}; do
        nohup $DIR/upload_files.sh $ip $local_path "$remote_path" $remove > $LOGPATH/$ip.log 2>&1 &
    done

    while [ $((`ps -ef | grep upload_files.sh | grep -v batch_upload_files.sh | grep -v grep | wc -l`)) -gt 0 ]; do
        echo "uploading....."
        sleep 1
    done
    echo "done!"

    retry_ip=()
    for src_ip in ${uploading_iplist[@]};
    do
        if [ $((`cat "$LOGPATH/$src_ip.log" | grep -v UTF | grep -v upload | grep -v echo | grep -v "Permanently added" | wc -l`)) -eq 0 ]; then
            echo "$src_ip upload success"
        else
            retry_ip+=($src_ip)

            echo "Error. $src_ip uploaded failed. Due to:"
            echo `cat "$LOGPATH/$src_ip.log"`
        fi
    done
    
    echo "retrylist=${retry_ip[*]}"
    uploading_iplist=(${retry_ip[*]})
done