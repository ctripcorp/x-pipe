package com.ctrip.xpipe.redis.console.migration.auto;

import com.ctrip.xpipe.api.migration.auto.MonitorService;

import java.util.Map;

/**
 * @author lishanglin
 * date 2021/1/15
 */
public interface MonitorServiceManager {

    MonitorService getOrCreate(long orgId);

    Map<Long, MonitorService> getAllServices();

}
