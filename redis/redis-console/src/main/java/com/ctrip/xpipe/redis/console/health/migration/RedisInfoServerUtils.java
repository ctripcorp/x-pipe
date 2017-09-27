package com.ctrip.xpipe.redis.console.health.migration;

import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author chen.zhu
 * <p>
 * Sep 27, 2017
 */
public class RedisInfoServerUtils {

    private static final String COLON_SPLITTER = "\\s*:\\s*";

    public static String getValueByKey(String InfoServer, String key) {
        String[] lines = StringUtil.splitByLineRemoveEmpty(InfoServer);
        for(String line : lines) {
            line = line.trim();
            if(line.startsWith(key)) {
                String[] keyAndVal = StringUtil.splitRemoveEmpty(COLON_SPLITTER, line);
                return keyAndVal[1];
            }
        }
        return null;
    }

    public static String getRedisVersion(String InfoServer) {
        return getValueByKey(InfoServer, "redis_version");
    }

    public static String getXRedisVersion(String InfoServer) {
        return getValueByKey(InfoServer, "xredis_version");
    }
}
