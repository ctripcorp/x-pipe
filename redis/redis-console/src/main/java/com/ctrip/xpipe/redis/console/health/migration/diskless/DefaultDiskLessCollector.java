package com.ctrip.xpipe.redis.console.health.migration.diskless;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.Sample;
import com.ctrip.xpipe.redis.console.health.migration.RedisInfoServerUtils;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2017
 */
@Component
@Lazy
public class DefaultDiskLessCollector implements DiskLessCollector {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private AlertManager alertManager;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Override
    public void collect(Sample<DiskLessInstanceResult> sample) {
        DiskLessSamplePlan samplePlan = (DiskLessSamplePlan) sample.getSamplePlan();
        String clusterId = samplePlan.getClusterId();
        String shardId = samplePlan.getShardId();

        samplePlan.getHostPort2SampleResult().forEach((hostPort, sampleResult) -> {
            if(sampleResult.isSuccess()) {
                checkRedisDiskLess(hostPort, sampleResult.getContext(), clusterId, shardId);
            } else {
                logger.error("Getting Redis info and conf, execution error: {}", sampleResult.getFailReason());
            }
        });
    }

    private void checkRedisDiskLess(HostPort hostPort, RedisInfoAndConf redisInfoAndConf, String clusterId, String shardId) {
        if(versionMatches(redisInfoAndConf.getServerInfo()) && isReplDiskLessSync(redisInfoAndConf.getServerConf())) {
            String message = String.format("Redis %s with version %s should not set %s as YES",
                    hostPort.toString(), consoleConfig.getRedisAlertVersion(), DiskLessMonitor.REPL_DISKLESS_SYNC);
            alertManager.alert(clusterId, shardId, ALERT_TYPE.REDIS_CONF_NOT_VALID, message);
        }
    }

    private boolean isReplDiskLessSync(List<String> serverConf) {
        logger.debug("[isReplDiskLessSync]config is as: {}", serverConf);
        try {
            String key = serverConf.get(0);
            String val = serverConf.get(1);
            if (key != null && key.trim().equalsIgnoreCase(DiskLessMonitor.REPL_DISKLESS_SYNC)) {
                if (val != null && val.trim().equalsIgnoreCase("yes"))
                    return true;
            }
            return false;
        } catch (Exception e) {
            logger.error("[isReplDiskLessSync]Error analysis redis server conf: {}", serverConf);
        }
        return false;
    }

    private boolean versionMatches(String serverInfo) {
        logger.debug("[versionMatches]Redis info server command result: \n {}", serverInfo);
        String targetVersion = consoleConfig.getRedisAlertVersion();
        String version = RedisInfoServerUtils.getRedisVersion(serverInfo);
        logger.debug("[versionMatches]Redis version is {}", version);
        logger.debug("[versionMatches]Redis alert version is {}", targetVersion);
        return version != null && StringUtil.compareVersion(version, targetVersion) < 1;
    }
}
