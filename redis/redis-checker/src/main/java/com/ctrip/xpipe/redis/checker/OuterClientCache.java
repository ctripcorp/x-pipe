package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.api.migration.OuterClientService;

import java.util.Map;

/**
 * @author lishanglin
 * date 2022/7/18
 */
public interface OuterClientCache {

    OuterClientService.ClusterInfo getClusterInfo(String clusterName) throws Exception;

    Map<String, OuterClientService.ClusterInfo> getAllActiveDcClusters(String activeDc);

}
