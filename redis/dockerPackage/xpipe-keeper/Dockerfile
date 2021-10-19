FROM java:8
COPY redis-keeper*.jar /xpipe-keeper/keeper.jar
ADD config/ /xpipe-keeper/config/
COPY scripts/docker-entrypoint.sh /usr/local/bin/

RUN chmod a+x /xpipe-keeper/keeper.jar /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["docker-entrypoint.sh"]