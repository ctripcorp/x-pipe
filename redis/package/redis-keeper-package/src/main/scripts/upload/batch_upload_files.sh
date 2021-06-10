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
        if [ $((`cat "$LOGPATH/$src_ip.log" | grep -v UTF | grep -v upload | grep -v echo | grep -v "Permanently added" | wc -l`)) -ne 0 ]; then
            retry_ip+=($src_ip)

            echo "Error. $src_ip uploaded failed. Due to:"
            echo `cat "$LOGPATH/$src_ip.log"`
        fi
    done
    
    echo "retrylist=${retry_ip[*]}"
    uploading_iplist=(${retry_ip[*]})
done

#    git_cmd="git clone http://git.dev.sh.ctripcorp.com/yq.ai/quick-start.git"
#    sshpass -p"$password" ssh -o StrictHostKeyChecking=no $username@$ip $git_cmd > $LOGPATH/$ip.log 2>&1 &

    
    #sshpass -p"$password" ssh -o StrictHostKeyChecking=no $username@$ip "pwd; ls" > $LOGPATH/$ip.log 2>&1 &

