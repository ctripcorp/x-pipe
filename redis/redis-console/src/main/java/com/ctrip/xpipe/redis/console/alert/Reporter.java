package com.ctrip.xpipe.redis.console.alert;

import java.util.Map;
import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Oct 13, 2017
 */
public interface Reporter {
    void immediateReport(RedisAlert redisAlert);
    void scheduledReport(Map<ALERT_TYPE, Set<RedisAlert>> redisAlerts);
}
