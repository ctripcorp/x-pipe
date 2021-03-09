package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterInstanceInfo;

/**
 * @author lishanglin
 * date 2021/1/14
 */
public class DefaultClusterHealthCheckInstance extends AbstractHealthCheckInstance<ClusterInstanceInfo> implements ClusterHealthCheckInstance {

    @Override
    public String toString() {
        return String.format("ClusterHealthCheckInstance[InstanceInfo: [%s]]", getCheckInfo().toString());
    }

}
