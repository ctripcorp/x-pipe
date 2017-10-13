package com.ctrip.xpipe.redis.console.health.redisconf.version;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.Sample;
import com.ctrip.xpipe.redis.console.health.redisconf.RedisConf;
import com.ctrip.xpipe.redis.console.health.redisconf.RedisConfManager;
import com.ctrip.xpipe.redis.console.health.redisconf.RedisInfoServerUtils;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
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

    @Autowired
    private RedisConfManager redisConfManager;

    @Autowired
    private MetaCache metaCache;

    @Override
    public void collect(Sample<VersionInstanceResult> result) {
        VersionSamplePlan samplePlan = (VersionSamplePlan) result.getSamplePlan();
        String clusterId = samplePlan.getClusterId();
        String shardId = samplePlan.getShardId();

        samplePlan.getHostPort2SampleResult().forEach((hostPort, sampleResult) -> {
            if(sampleResult.isSuccess()) {
                cacheRedisInfo(hostPort, sampleResult.getContext());
                checkRedisVersion(hostPort, sampleResult.getContext(), clusterId, shardId);
            } else {
                logger.error("Getting Redis Version, execution error: {}", sampleResult.getFailReason());
            }
        });
    }

    void checkRedisVersion(HostPort hostPort, String message, String clusterId, String shardId) {
        if(!isRedisInBackupDC(hostPort)) {
            logger.debug("[checkRedisVersion]Redis {} is not in backup dc");
            return;
        }
        logger.debug("[checkRedisVersion]Redis {}: Server Info: \n{}", hostPort, message);
        String targetVersion = consoleConfig.getXRedisMinimumRequestVersion();
        String version = RedisInfoServerUtils.getXRedisVersion(message);
        logger.debug("[checkRedisVersion]Current Redis {} xredis_version: {}", hostPort, version);
        if(version == null || StringUtil.compareVersion(version, targetVersion) < 0) {
            String alertMessage = String.format("Redis %s should be XRedis 0.0.3 or above",  hostPort.toString());
            logger.warn("{}", alertMessage);
            alertManager.alert(clusterId, shardId, hostPort, ALERT_TYPE.XREDIS_VERSION_NOT_VALID, alertMessage);
        }
    }

    private void cacheRedisInfo(HostPort hostPort, String info) {
        String redisVersion = RedisInfoServerUtils.getRedisVersion(info);
        String xredisVersion = RedisInfoServerUtils.getXRedisVersion(info);
        logger.debug("[cacheRedisInfo] Cache Redis {}, Redis Version: {}, XRedis Version: {}",
                hostPort, redisVersion, xredisVersion);
        RedisConf redisConf = redisConfManager.findOrCreateConfig(hostPort.getHost(), hostPort.getPort());
        redisConf.setRedisVersion(redisVersion);
        redisConf.setXredisVersion(xredisVersion);
    }

    private boolean isRedisInBackupDC(HostPort hostPort) {
        return metaCache.inBackupDc(hostPort);
    }
}
