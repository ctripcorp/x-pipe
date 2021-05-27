#!/bin/bash

###############################copy cert####################
CUR_DIR="$PWD"
CERT_DIR=$CUR_DIR/../cert
ls $CERT_DIR
mkdir -p /opt/data/100013684/openssl
chown deploy:deploy /opt/data/100013684/openssl
cp $CERT_DIR/* /opt/data/100013684/openssl
chown deploy:deploy /opt/data/100013684/openssl/*

###############################reset xpipe.properties###########
if [ -f "/opt/data/100013684/xpipe.properties" ]; then
  rm /opt/data/100013684/xpipe.properties
fi

echo 'proxy.frontend.tcp.port = 80
proxy.frontend.tls.port = 443
proxy.endpoint.check.interval.sec = 2
proxy.traffic.report.interval.milli = 5000
proxy.no.tls.netty.handler = false
proxy.internal.network.prefix = 10
proxy.recv.buffer.size = 4096
proxy.root.file.path = /opt/data/100013684/openssl/ca.crt
proxy.server.cert.chain.file.path = /opt/data/100013684/openssl/server.crt
proxy.client.cert.chain.file.path = /opt/data/100013684/openssl/client.crt
proxy.server.key.file.path = /opt/data/100013684/openssl/pkcs8_server.key
proxy.client.key.file.path = /opt/data/100013684/openssl/pkcs8_client.key' >> /opt/data/100013684/xpipe.properties

