ip=$1

local_ip=`ifconfig | grep broadcast | awk '{print $2}'`

if [ "$ip" != "$local_ip" ]; then
    iptables -w -A INPUT -p all --src $ip -j DROP
    iptables -w -A OUTPUT -p all --src $ip -j DROP
fi
