package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.utils.StringUtil;

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
            boolean isCurrentDc = currentDcId.equalsIgnoreCase(instance.getCheckInfo().getActiveDc());
            String azGroupType = instance.getCheckInfo().getAzGroupType();
            if (StringUtil.isEmpty(azGroupType) || ClusterType.lookup(azGroupType) != ClusterType.SINGLE_DC) {
                return isCurrentDc;
            } else {
                return !isCurrentDc;
            }
        }
        return true;
    }

}
