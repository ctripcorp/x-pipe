package com.ctrip.xpipe.redis.console.alert;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.utils.MapUtils;
import io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Oct 13, 2017
 */
@Component
public class DefaultRedisAlertManager implements RedisAlertManager {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduledExecutor;

    private ConcurrentMap<ALERT_TYPE, ConcurrentSet<RedisAlert>> redisAlerts = new ConcurrentHashMap<>();

    private ConcurrentMap<ALERT_TYPE, ConcurrentSet<RedisAlert>> backupRedisAlerts;

    @Autowired
    List<Reporter> reporters;

    @PostConstruct
    public void scheduledTask() {
        int initialDelay = 10, period = 30;
        scheduledExecutor.scheduleAtFixedRate(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                try {
                    synchronized (this) {
                        final Map<ALERT_TYPE, Set<RedisAlert>> map = new HashMap<>(redisAlerts);
                        reporters.forEach(reporter -> reporter.scheduledReport(map));
                        cleanup();
                    }
                } catch (Exception e) {
                    logger.error("[scheduledTask]{}", e);
                }
            }
        }, initialDelay, period, TimeUnit.MINUTES);
    }

    protected void cleanup() {
        backupRedisAlerts = redisAlerts;
        redisAlerts = new ConcurrentHashMap<>();
    }

    @Override
    public RedisAlert findOrCreateRedisAlert(ALERT_TYPE alertType, String clusterId,
                                             String shardId, HostPort hostPort, String message) {
        ConcurrentSet<RedisAlert> typedRedisAlerts = MapUtils.getOrCreate(redisAlerts,
                alertType, () -> new ConcurrentSet<>());
        ConcurrentSet<RedisAlert> typedBackupRedisAlerts = backupRedisAlerts == null ? null
                :MapUtils.getOrCreate(backupRedisAlerts, alertType, () -> new ConcurrentSet<>());

        RedisAlert redisAlert = createRedisAlert(clusterId, shardId, hostPort, alertType, message);
        if(typedRedisAlerts.add(redisAlert) &&
                (backupRedisAlerts != null && !typedBackupRedisAlerts.contains(redisAlert))) {
            logger.info("[findOrCreateRedisAlert]New Issue occurs, report immediately");
            reporters.forEach(reporter -> reporter.immediateReport(redisAlert));
        } else {
            logger.info("[findOrCreateRedisAlert]RedisAlert {} already exists", redisAlert);
        }
        return redisAlert;
    }


    private RedisAlert createRedisAlert(String cluster, String shard, HostPort hostPort,
                                        ALERT_TYPE alertType, String message) {
        return new RedisAlert.RedisAlertBuilder()
                .alertType(alertType)
                .clusterId(cluster)
                .shardId(shard)
                .message(message)
                .hostPort(hostPort)
                .createRedisAlert();
    }
}
