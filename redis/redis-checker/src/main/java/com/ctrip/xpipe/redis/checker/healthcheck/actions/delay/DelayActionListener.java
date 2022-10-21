package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Oct 11, 2018
 */
public interface DelayActionListener extends RedisHealthCheckActionListener<DelayActionContext> {

    String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof DelayActionContext;
    }

    default boolean supportInstance(RedisHealthCheckInstance instance) {
        ClusterType clusterType = instance.getCheckInfo().getClusterType();
        if (clusterType.supportSingleActiveDC()) {
            return currentDcId.equalsIgnoreCase(instance.getCheckInfo().getActiveDc()) == DcGroupType.isNullOrDrMaster(instance.getCheckInfo().getDcGroupType());
        }
        return true;
    }

}
