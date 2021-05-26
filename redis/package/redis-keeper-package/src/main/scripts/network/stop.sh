DIR=`dirname $0`
LOGPATH="$DIR/logs/recovery"
mkdir -p $LOGPATH

IPLIST_PATH="$DIR/iplist"

username=xpipe
password=12qwaszx

#recovery network
recovering_src_iplist=()
while read src_ip
do
    recovering_src_iplist+=($src_ip)
done < $IPLIST_PATH

sudo_recovery_cmd="echo $password | sudo -S iptables -F"

while [ ${#recovering_src_iplist[@]} -gt 0 ]; do
    for src_ip in ${recovering_src_iplist[@]};
    do
        nohup sshpass -p"$password" ssh -o StrictHostKeyChecking=no $username@$src_ip $sudo_recovery_cmd > "$LOGPATH/$src_ip.log" 2>&1 &
    done

    echo "recovering.....\c"
    while [ $((`ps -ef | grep sshpass | grep -v grep | wc -l`)) -gt 0 ]; do
        echo ".....\c"
        sleep 0.5
    done
    echo "done!"

    retry_ip=()
    for src_ip in ${recovering_src_iplist[@]};
    do
        if [ $((`cat "$LOGPATH/$src_ip.log" | grep -v UTF| grep -v "Permanently added" | wc -l`)) -eq 0 ]; then
            echo "$src_ip recovered success"
        else
            retry_ip+=($src_ip)
            echo "Error. $src_ip recovered failed. Due to:"
            echo `cat "$LOGPATH/$src_ip.log"`
        fi
    done
    echo "retrylist=${retry_ip[*]}"
    recovering_src_iplist=(${retry_ip[*]})
done

#cancel cron
$DIR/trigger_add_cron.sh cancel
