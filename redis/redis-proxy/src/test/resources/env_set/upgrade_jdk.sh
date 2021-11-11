#!/bin/bash

#if [ -f /opt/settings/server.properties ];then
#    IDC=`cat /opt/settings/server.properties | egrep -i "^idc" | awk -F= '{print $2}'`
#    if [ $IDC != "SHA-ALI" ];then
#        yum remove jdk11
#        rpm -i http://git.dev.sh.ctripcorp.com/baseimage/components/raw/master/jdk11-0.4-1.x86_64.rpm
#        /usr/java/jdk11/bin/java -version
#    fi
#fi

# enable app bind port at 80 and 443
if [[ -z "$JAVA_HOME" && -d /usr/java/jdk11/ ]]; then
	setcap 'cap_net_bind_service=+ep' /usr/java/jdk11/bin/java
elif [[ -z "$JAVA_HOME" && -d /usr/java/latest/ ]]; then
	setcap 'cap_net_bind_service=+ep' /usr/java/latest/bin/java
elif [[ -n "$JAVA_HOME" ]]; then
	setcap 'cap_net_bind_service=+ep' $JAVA_HOME/bin/java
fi

touch /etc/ld.so.conf.d/java.conf

sed -i '1,$d' /etc/ld.so.conf.d/java.conf

echo '/usr/java/jdk11/lib/jli/' >> /etc/ld.so.conf.d/java.conf

ldconfig | grep libjli

/usr/java/jdk11/bin/java -version