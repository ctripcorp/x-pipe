package com.ctrip.xpipe.redis.console.health.migration.version;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.migration.Callbackable;
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
 * Sep 20, 2017
 */

@Component
@Lazy
public class VersionCallback implements Callbackable<String> {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private final static String SPLITTER = System.lineSeparator();

    private final static String REDIS_VERSION = "redis_version";

    private final static String REDIS_VERSION_SPLITTER = ":";

    @Autowired
    protected ConsoleConfig consoleConfig;

    @Override
    public void success(String message) {
        String version = getRedisVersion(message);
        String alertVersion = consoleConfig.getRedisAlertVersion();
        if(ObjectUtils.equals(version, alertVersion)) {
            throw new IllegalStateException("version error");
        }
    }

    public String getRedisVersion(String info) {
        String versionNumber = "";
        try {
            String[] redisInfo = StringUtil.splitRemoveEmpty(SPLITTER, info);
            String version = getRedisVersion(redisInfo);
            if (version != null) {
                versionNumber = version.split(REDIS_VERSION_SPLITTER)[1];
            }
        } catch (Exception e) {
            logger.error("[version] Error Analysis String", e);
        }
        return versionNumber;
    }

    private String getRedisVersion(String[] redisInfo) {
        for(String info : redisInfo) {
            if(info.contains(REDIS_VERSION)) {
                return info;
            }
        }
        return null;
    }

    @Override
    public void fail(Throwable th) {
        System.out.println(th.getMessage());
    }
}
