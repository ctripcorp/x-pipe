FROM java:8
COPY redis-meta*.jar /xpipe-meta/meta.jar
ADD config /xpipe-meta/config
COPY scripts/docker-entrypoint.sh /usr/local/bin/

RUN chmod a+x /xpipe-meta/meta.jar /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["docker-entrypoint.sh"]