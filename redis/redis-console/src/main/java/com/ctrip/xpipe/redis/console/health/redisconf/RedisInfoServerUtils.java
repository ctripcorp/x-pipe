package com.ctrip.xpipe.redis.console.health.redisconf;

import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author chen.zhu
 * <p>
 * Sep 27, 2017
 */
public class RedisInfoServerUtils {

    private static final String COLON_SPLITTER = "\\s*:\\s*";

    public static String getValueByKey(String infoServer, String key) {
        String[] lines = StringUtil.splitByLineRemoveEmpty(infoServer);
        for(String line : lines) {
            line = line.trim();
            if(line.startsWith(key)) {
                String[] keyAndVal = StringUtil.splitRemoveEmpty(COLON_SPLITTER, line);
                return keyAndVal[1];
            }
        }
        return null;
    }

    public static String getRedisVersion(String infoServer) {
        return getValueByKey(infoServer, "redis_version");
    }

    public static String getXRedisVersion(String infoServer) {
        return getValueByKey(infoServer, "xredis_version");
    }
}
