package com.ctrip.xpipe.redis.checker;

import java.util.List;
import java.util.Set;

public class TestDcRelationsService implements DcRelationsService {
    @Override
    public List<String> getTargetDcsByPriority(String clusterName, String downDc, List<String> availableDcs) {
        return null;
    }

    @Override
    public Integer getDcsDelay(String fromDc, String toDc) {
        return null;
    }

    @Override
    public Integer getClusterDcsDelay(String clusterName, String fromDc, String toDc) {
        return null;
    }

    @Override
    public Set<String> getExcludedDcsForBiCluster(String clusterName, Set<String> downDcs, Set<String> availableDcs) {
        return null;
    }

}
