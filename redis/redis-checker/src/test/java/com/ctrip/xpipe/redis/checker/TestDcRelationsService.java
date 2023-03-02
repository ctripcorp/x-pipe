package com.ctrip.xpipe.redis.checker;

import java.util.List;

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
}
