DIR=`dirname $0`
LOGPATH="$DIR/logs/check_time"
iplist_file=$1

mkdir -p $LOGPATH

username=xpipe
password=12qwaszx

kill_ntp_cmd="ps -ef | grep /usr/sbin/ntpd | grep -v grep | awk '{print \\\$2}' | xargs kill -9"
sudo_kill_ntp_cmd="echo $password | sudo -S bash -c \"$kill_ntp_cmd\""

fix_time_cmd='ntpdate it_ntp00.qa.nt.ctripcorp.com'
sudo_fix_time_cmd="echo $password | sudo -S $fix_time_cmd"

start_ntp_cmd="/usr/sbin/ntpd -u ntp:ntp -g"
sudo_start_ntp_cmd="echo $password | sudo -S $start_ntp_cmd"

restart_crontab_cmd="service crond restart"
sudo_restart_crontab_cmd="echo $password | sudo -S $restart_crontab_cmd"

iplist=()
while read ip
do
    iplist+=($ip)
done < $iplist_file
origin_iplist=(${iplist[*]})

#stop ntp and update time
echo "----------------------stop ntp and synchronize time--------------------------"
while [ ${#iplist[@]} -gt 0 ]; do
    for ip in ${iplist[@]};
    do
        nohup sshpass -p"$password" ssh -o StrictHostKeyChecking=no $username@$ip "$sudo_kill_ntp_cmd;$sudo_fix_time_cmd" > $LOGPATH/$ip.log 2>&1 &
    done

    count=0
    while [ $((`ps -ef | grep sshpass | wc -l`)) -gt 1 ]; do
        if [ $(($count%6)) -eq 1 ]; then
            echo "waiting ips:"
            echo `ps -ef | grep sshpass | awk '{print $10,$13}' | grep -v "^grep"| awk '{print $2}' | cut -b 7-`
        fi
        count=$(($count+1))
        sleep 0.5
    done

    retry_ip=()
    for ip in ${iplist[@]};
    do
        if [ $((`cat "$LOGPATH/$ip.log" | grep "adjust time server\|step time server" | wc -l`)) -eq 0 ]; then
            echo "$ip failed due to"
            cat $LOGPATH/$ip.log
            retry_ip+=($ip)
        fi
    done
    echo "retrylist=${retry_ip[*]}"
    iplist=(${retry_ip[*]})
done

#start ntp and check ntp already started
echo "----------------------start ntp and check ntp already started------------------------------------"
iplist=(${origin_iplist[*]})
while [ ${#iplist[@]} -gt 0 ]; do
    for ip in ${iplist[@]};
    do
        nohup sshpass -p"$password" ssh -o StrictHostKeyChecking=no $username@$ip "$sudo_start_ntp_cmd;$sudo_fix_time_cmd" >> $LOGPATH/$ip.log 2>&1 &
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

#restart crontab
#For preventing cases: eg.now 20:00:00, cron triggered on 20:05:00, if I change time to 20:10:00, then change back to real time, the cron task won't be triggerd
#This case can be solved by restarting crontab
echo "----------------------restart crontab------------------------------------"
iplist=(${origin_iplist[*]})
while [ ${#iplist[@]} -gt 0 ]; do
    for ip in ${iplist[@]};
    do
        nohup sshpass -p"$password" ssh -o StrictHostKeyChecking=no $username@$ip "$sudo_restart_crontab_cmd" >> $LOGPATH/$ip.log 2>&1 &
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
        if [ $((`cat "$LOGPATH/$ip.log" | grep "Redirecting to /bin/systemctl restart crond.service" | wc -l `)) -eq 0 ]; then
            echo "$ip failed due to"
            cat $LOGPATH/$ip.log
            retry_ip+=($ip)
        fi
    done
    echo "retrylist=${retry_ip[*]}"
    iplist=(${retry_ip[*]})
done
