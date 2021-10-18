FROM java:8
COPY redis-console*.jar /xpipe-console/console.jar
ADD config/ /xpipe-console/config/
COPY scripts/docker-entrypoint.sh /usr/local/bin/

RUN chmod a+x /xpipe-console/console.jar /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["docker-entrypoint.sh"]