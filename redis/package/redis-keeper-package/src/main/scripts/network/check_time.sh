DIR=`dirname $0`
LOGPATH="$DIR/logs/check_time"
iplist_file=$1

mkdir -p $LOGPATH

username=xpipe
password=12qwaszx

fix_time_cmd='ntpdate it_ntp00.qa.nt.ctripcorp.com'
sudo_fix_time_cmd="echo $password | sudo -S $fix_time_cmd"

iplist=()
while read ip
do
    iplist+=($ip)
done < $iplist_file

while [ ${#iplist[@]} -gt 0 ]; do
    for ip in ${iplist[@]};
    do
        nohup sshpass -p"$password" ssh -o StrictHostKeyChecking=no $username@$ip $sudo_fix_time_cmd > $LOGPATH/$ip.log 2>&1 &
    done


    count=0
    while [ $((`ps -ef | grep sshpass | wc -l`)) -gt 1 ]; do
        if [ $(($count%6)) -eq 1 ]; then
            echo "waiting ips:"
            echo `ps -ef | grep sshpass | grep -v grep | awk '{print $13}' | cut -b 7-`
        fi
        count=$(($count+1))
        sleep 0.5
    done

    retry_ip=()
    for ip in ${iplist[@]};
    do
        if [ $((`cat "$LOGPATH/$ip.log" | grep "the NTP socket is in use, exiting" | wc -l `)) -eq 0 ]; then
            echo "$ip failed due to"
            cat $LOGPATH/$ip.log
            retry_ip+=($ip)
        fi
    done
    echo "retrylist=${retry_ip[*]}"
    iplist=(${retry_ip[*]})
done
