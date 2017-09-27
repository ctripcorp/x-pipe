package com.ctrip.xpipe.redis.console.health.migration.version;

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

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2017
 */

@Component
@Lazy
public class DefaultVersionCollector implements VersionCollector {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private AlertManager alertManager;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Override
    public void collect(Sample<VersionInstanceResult> result) {
        VersionSamplePlan samplePlan = (VersionSamplePlan) result.getSamplePlan();
        String clusterId = samplePlan.getClusterId();
        String shardId = samplePlan.getShardId();

        samplePlan.getHostPort2SampleResult().forEach((hostPort, sampleResult) -> {
            if(sampleResult.isSuccess()) {
                checkRedisVersion(hostPort, sampleResult.getContext(), clusterId, shardId);
            } else {
                logger.error("Getting Redis Version, execution error: {}", sampleResult.getFailReason());
            }
        });
    }

    void checkRedisVersion(HostPort hostPort, String message, String clusterId, String shardId) {
        logger.debug("[checkRedisVersion]Redis {}: Server Info: \n{}", hostPort, message);
        String targetVersion = consoleConfig.getXRedisMinimumRequestVersion();
        String version = RedisInfoServerUtils.getXRedisVersion(message);
        logger.debug("[checkRedisVersion]Current Redis {} xredis_version: {}", hostPort, version);
        if(version == null || StringUtil.compareVersion(version, targetVersion) < 0) {
            String alertMessage = String.format("Redis %s should be XRedis",  hostPort.toString());
            logger.warn("{}", alertMessage);
            alertManager.alert(clusterId, shardId, ALERT_TYPE.REDIS_VERSION_NOT_VALID, alertMessage);
        }
    }
}
