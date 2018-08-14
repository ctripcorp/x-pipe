package com.ctrip.xpipe.redis.console.health.redisconf.diskless;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.Sample;
import com.ctrip.xpipe.redis.console.health.redisconf.RedisConf;
import com.ctrip.xpipe.redis.console.health.redisconf.RedisConfManager;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

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

    @Autowired
    private RedisConfManager redisConfManager;

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

    private void checkRedisDiskLess(HostPort hostPort, boolean isDisklessSync, String clusterId, String shardId) {
        if(versionMatches(hostPort) && isDisklessSync) {
            String message = String.format("Redis %s should not set %s as YES",
                    hostPort.toString(), DiskLessMonitor.REPL_DISKLESS_SYNC);
            alertManager.alert(clusterId, shardId, hostPort, ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR, message);
        }
    }

    @VisibleForTesting
    protected boolean versionMatches(HostPort hostPort) {
        RedisConf redisConf = redisConfManager.findOrCreateConfig(hostPort.getHost(), hostPort.getPort());
        String targetVersion = consoleConfig.getReplDisklessMinRedisVersion();
        String version = redisConf.getRedisVersion();
        logger.debug("[versionMatches]Redis {} version is {}", hostPort, version);
        logger.debug("[versionMatches]Redis {} alert version is {}", hostPort, targetVersion);
        return version != null && StringUtil.compareVersion(version, targetVersion) < 1;
    }
}
