FROM java:8
COPY redis-proxy*.jar /xpipe-proxy/proxy.jar
COPY config/ /xpipe-proxy/config/
COPY scripts/docker-entrypoint.sh /usr/local/bin/
#传递证书
COPY cert/ /opt/data/100013684/openssl/

RUN chmod a+x /xpipe-proxy/proxy.jar /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["docker-entrypoint.sh"]