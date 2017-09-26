package com.ctrip.xpipe.redis.console.health.migration.version;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.Sample;
import com.ctrip.xpipe.utils.ObjectUtils;
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

    private static final String LINE_SPLITTER = System.lineSeparator();

    private static final String REDIS_VERSION_KEY = "redis_version";

    private static final String REDIS_VERSION_SPLITTER = "\\s*:\\s*";

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

    private void checkRedisVersion(HostPort hostPort, String message, String clusterId, String shardId) {
        logger.debug("Redis {}: Server Info: \n{}", hostPort, message);
        String targetVersion = consoleConfig.getRedisAlertVersion();
        logger.debug("Alert version for redis is: {}", targetVersion);
        String currentRedisVersion = getRedisVersion(message);
        logger.debug("Current Redis {} version: {}", hostPort, currentRedisVersion);
        if(ObjectUtils.equals(currentRedisVersion, targetVersion)) {
            String alertMessage = String.format("Redis Server: %s version is %s, which is not supported in backup DC",
                    hostPort.toString(), currentRedisVersion);
            logger.warn("{}", alertMessage);
            alertManager.alert(clusterId, shardId, ALERT_TYPE.REDIS_VERSION_NOT_VALID, alertMessage);
        }
    }

    // Change to protected to do unit test
    public static String getRedisVersion(String message) {
        String[] serverInfo = StringUtil.splitRemoveEmpty(LINE_SPLITTER, message);
        for(String info : serverInfo) {
            if(info.contains(REDIS_VERSION_KEY)) {
                String[] strs = StringUtil.splitRemoveEmpty(REDIS_VERSION_SPLITTER, info);
                return strs[1];
            }
        }
        return null;
    }
}
