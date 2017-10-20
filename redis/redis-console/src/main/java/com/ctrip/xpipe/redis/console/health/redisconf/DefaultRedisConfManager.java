package com.ctrip.xpipe.redis.console.health.redisconf;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.console.health.HealthChecker;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Sep 28, 2017
 */
@Component
@ConditionalOnProperty(name = { HealthChecker.ENABLED }, matchIfMissing = true)
public class DefaultRedisConfManager implements RedisConfManager {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private ConcurrentMap<HostPort, RedisConf> configs = new ConcurrentHashMap<>();

    @Autowired
    MetaCache metaCache;

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Override
    public RedisConf findOrCreateConfig(String host, int port) {
        HostPort hostPort = new HostPort(host, port);
        RedisConf conf = configs.get(hostPort);

        if (conf == null) {
            synchronized (this) {
                conf = configs.get(hostPort);
                if (conf == null) {
                    conf = buildRedisConf(hostPort);
                    configs.put(hostPort, conf);
                }
            }
        }

        return conf;
    }

    private RedisConf buildRedisConf(HostPort hostPort) {
        Pair<String, String> clusterShard = metaCache.findClusterShard(hostPort);
        String clusterId = clusterShard.getKey();
        String shardId = clusterShard.getValue();
        return new RedisConf(hostPort, clusterId, shardId);
    }

    @PostConstruct
    public void postConstruct() {
        int initialDelay = 5;
        int period = 1;
        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                String eventType = "Redis.Server.Version";
                try {
                    for(ConcurrentMap.Entry entry : configs.entrySet()) {
                        CatEventMonitor.DEFAULT.logEvent(eventType, entry.getValue().toString());
                    }
                    logger.debug("Current Redis Conf Manager Cache:\n {}", this.toString());
                } catch (Exception e) {
                    logger.error("[postConstruct]{}", e);
                }
            }
        }, initialDelay, period, TimeUnit.MINUTES);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(RedisConf redisConf : configs.values()) {
            sb.append(redisConf.toString());
            sb.append("\n");
        }
        return sb.toString();
    }

}
