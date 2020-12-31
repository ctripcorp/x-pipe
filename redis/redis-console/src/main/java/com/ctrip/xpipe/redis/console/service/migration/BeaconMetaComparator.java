package com.ctrip.xpipe.redis.console.service.migration;

import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.GroupStatusVO;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterNotFoundException;

import java.util.List;

/**
 * @author lishanglin
 * date 2020/12/28
 */
public interface BeaconMetaComparator {

    boolean compareWithXPipe(String clusterName, List<GroupStatusVO> beaconClusterMeta) throws ClusterNotFoundException;

}
