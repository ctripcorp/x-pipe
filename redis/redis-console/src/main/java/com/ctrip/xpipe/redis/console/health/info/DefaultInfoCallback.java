package com.ctrip.xpipe.redis.console.health.info;

import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.Arrays;
import java.util.concurrent.Executor;

/**
 * @author chen.zhu
 * <p>
 * Sep 20, 2017
 */
public class DefaultInfoCallback implements InfoCallback {

    private final static String SPLITTER = System.lineSeparator();

    private final static String REDIS_VERSION = "redis_version";

    private final static String REDIS_VERSION_SPLITTER = ":";

    @Override
    public String version(String info) {
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

    private void alertIfMatches(String versionNumber, String alertVersion) {
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
