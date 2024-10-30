package com.ctrip.xpipe.redis.console.migration.auto;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/15
 */
public interface MonitorManager {

    MonitorService get(long orgId, String clusterName);

    Map<Long, List<MonitorService>> getAllServices();

    Map<BeaconSystem, Map<Long, Set<String>>> clustersByBeaconSystemOrg();
}
