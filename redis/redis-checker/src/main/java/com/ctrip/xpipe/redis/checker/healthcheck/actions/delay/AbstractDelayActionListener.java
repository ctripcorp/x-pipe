package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

public abstract class AbstractDelayActionListener implements DelayActionListener{

    protected String currentDcId = FoundationService.DEFAULT.getDataCenter();

    public AbstractDelayActionListener() {
    }

    public AbstractDelayActionListener(String currentDcId) {
        this.currentDcId = currentDcId;
    }

    public boolean supportInstance(RedisHealthCheckInstance instance) {
        ClusterType clusterType = instance.getCheckInfo().getClusterType();
        if (clusterType.supportSingleActiveDC()) {
            return currentDcId.equalsIgnoreCase(instance.getCheckInfo().getActiveDc()) == DcGroupType.isNullOrDrMaster(instance.getCheckInfo().getDcGroupType());
        }
        return true;
    }

}
