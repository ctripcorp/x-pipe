package com.ctrip.xpipe.redis.checker;

import java.util.List;
import java.util.Set;

public interface DcRelationsService {

    List<String> getTargetDcsByPriority(String clusterName, String downDc, List<String> availableDcs);

    Set<String> getExcludedDcsForBiCluster(String clusterName, Set<String> downDcs, Set<String> availableDcs);

    Integer getDcsDelay(String fromDc, String toDc);

    Integer getClusterDcsDelay(String clusterName, String fromDc, String toDc);

}
