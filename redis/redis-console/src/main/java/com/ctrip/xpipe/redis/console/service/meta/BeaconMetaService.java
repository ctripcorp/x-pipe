package com.ctrip.xpipe.redis.console.service.meta;

import com.ctrip.xpipe.redis.console.model.beacon.BeaconGroupModel;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterNotFoundException;

import java.util.Set;

/**
 * @author lishanglin
 * date 2020/12/31
 */
public interface BeaconMetaService {

    String BEACON_GROUP_SEPARATOR_REGEX = "\\+";

    String BEACON_GROUP_SEPARATOR = "+";

    Set<BeaconGroupModel> buildBeaconGroups(String cluster);

    boolean compareMetaWithXPipe(String clusterName, Set<BeaconGroupModel> beaconGroups) throws ClusterNotFoundException;

}
