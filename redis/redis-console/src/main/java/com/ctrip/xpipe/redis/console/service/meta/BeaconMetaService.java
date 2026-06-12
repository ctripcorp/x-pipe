package com.ctrip.xpipe.redis.console.service.meta;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorShardMeta;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterNotFoundException;

import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2020/12/31
 */
public interface BeaconMetaService {

    String BEACON_GROUP_SEPARATOR_REGEX = "\\+";

    String BEACON_GROUP_SEPARATOR = "+";

    Set<MonitorGroupMeta> buildDrBeaconGroups(String cluster, String dc);

    Set<MonitorShardMeta> buildSentinelBeaconShards(String cluster, String dc, Map<String, HostPort> shardMasters);

    boolean compareDrBeaconMetaWithXPipe(String clusterName, Set<MonitorGroupMeta> beaconGroups) throws ClusterNotFoundException;

    boolean compareDrBeaconMetaWithXPipe(String clusterName, String dc, Set<MonitorGroupMeta> beaconGroups)
            throws ClusterNotFoundException;

}
