package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.redis.console.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.ClusterInstanceInfo;

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
