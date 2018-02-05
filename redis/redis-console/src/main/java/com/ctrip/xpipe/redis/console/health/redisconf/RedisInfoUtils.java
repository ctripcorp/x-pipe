package com.ctrip.xpipe.redis.console.health.redisconf;

import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * Sep 27, 2017
 */
public class RedisInfoUtils {

    private static final String COLON_SPLITTER = "\\s*:\\s*";

    private static Logger logger = LoggerFactory.getLogger(RedisInfoUtils.class);

    public synchronized static String getValueByKey(String infoServer, String key) {
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

    public static boolean getReplBacklogActive(String infoReplication) {
        String backlogActive = getValueByKey(infoReplication, "repl_backlog_active");
        try {
            if(StringUtil.isEmpty(backlogActive)) {
                logger.warn("Did not get 'repl_backlog_active'");
            }
            int result = Integer.valueOf(backlogActive);
            if(result != 1) {
                return false;
            }
        } catch (Exception ignore) {
            logger.error("[getReplBacklogActive]", ignore);
        }
        return true;
    }

}
