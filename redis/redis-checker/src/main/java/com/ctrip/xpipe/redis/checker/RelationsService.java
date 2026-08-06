package com.ctrip.xpipe.redis.checker;

import java.util.List;
import java.util.Set;

public interface RelationsService {

    String getClusterTargetDcByPriority(long clusterId, String clusterName, String downDc, List<String> availableDcs);

    Set<String> getExcludedDcsForBiCluster(String clusterName, Set<String> downDcs, Set<String> availableDcs);

    Integer getDcsDelay(String fromDc, String toDc);

    Integer getClusterDcsDelay(String clusterName, String fromDc, String toDc);

    boolean isReachableRegion(String dc1, String dc2);

    Integer getRegionDelay(String fromDc, String toDc);

}
