package com.ctrip.xpipe.redis.console.service.meta;

import com.ctrip.xpipe.redis.console.beacon.data.BeaconGroupMeta;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterNotFoundException;

import java.util.Set;

/**
 * @author lishanglin
 * date 2020/12/31
 */
public interface BeaconMetaService {

    String BEACON_GROUP_SEPARATOR_REGEX = "\\+";

    String BEACON_GROUP_SEPARATOR = "+";

    Set<BeaconGroupMeta> buildBeaconGroups(String cluster);

    boolean compareMetaWithXPipe(String clusterName, Set<BeaconGroupMeta> beaconGroups) throws ClusterNotFoundException;

}
