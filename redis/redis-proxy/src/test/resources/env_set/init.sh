#!/bin/bash
 
 
########################### Clean & Set & Run#################################
 
sed -i '/net.core.wmem_max/d' /etc/sysctl.conf
 
sed -i '/net.core.rmem_max/d' /etc/sysctl.conf
 
sed -i '/net.ipv4.tcp_rmem/d' /etc/sysctl.conf
 
sed -i '/net.ipv4.tcp_wmem/d' /etc/sysctl.conf
 
sed -i '/net.core.default_qdisc/d' /etc/sysctl.conf
 
sed -i '/net.ipv4.tcp_congestion_control/d' /etc/sysctl.conf
 
 
 
 
echo 'net.core.wmem_max=50485760' >> /etc/sysctl.conf
 
echo 'net.core.rmem_max=50485760' >> /etc/sysctl.conf
 
echo 'net.ipv4.tcp_rmem = 4096 87380 50485760' >> /etc/sysctl.conf
 
echo 'net.ipv4.tcp_wmem = 4096 87380 50485760' >> /etc/sysctl.conf
 
echo 'net.core.default_qdisc=fq' >> /etc/sysctl.conf
 
echo 'net.ipv4.tcp_congestion_control=bbr' >> /etc/sysctl.conf
 
sysctl -p

# enable app bind port at 80 and 443
ARCH=`uname -r`
JAVA_PATH=/usr/java/jdk11
JLI_PATH=/usr/java/jdk11/lib/jli
if [[ "$ARCH" == *"aarch64" ]]; then
    JAVA_PATH=/usr/java/jdk17
    JLI_PATH=/usr/java/jdk17/lib
fi

setcap 'cap_net_bind_service=+ep' $JAVA_PATH/bin/java

touch /etc/ld.so.conf.d/java.conf

sed -i '1,$d' /etc/ld.so.conf.d/java.conf
 
echo $JLI_PATH >> /etc/ld.so.conf.d/java.conf
 
ldconfig | grep libjli
############################# Check ##################################
 
echo ''
 
echo 'Checking system params...'
 
congestion_control=`sysctl -a | grep "net.ipv4.tcp_congestion_control" | awk -F= '{print $2}'| sed 's/[[:space:]]//g'`
 
echo "$congestion_control"
 
if [ "$congestion_control" != "bbr" ]
 
then
 
    echo "[WARN] Congestion Control shoud bbr, but: $congestion_control"
 
fi
 
tcp_rmem=`sysctl -a | grep "net.ipv4.tcp_rmem" | awk -F"[=  *]" '{print $4}'`
 
echo "$tcp_rmem"
 
if [ "$tcp_rmem" != "4096   87380   50485760" ]
 
then
 
    echo "[WARN] net.ipv4.tcp_rmem shoud be: 4096 87380 50485760, but: $tcp_rmem"
 
fi
 
tcp_wmem=`sysctl -a | grep "net.ipv4.tcp_wmem" | awk -F= '{print $2}'`
 
echo "$tcp_wmem"
 
if [ "$tcp_wmem" != " 4096  87380   50485760" ]
 
then
 
    echo "[WARN] net.ipv4.tcp_wmem shoud be: 4096 87380 50485760, but: $tcp_wmem"
 
fi
/usr/java/latest/bin/java -version
