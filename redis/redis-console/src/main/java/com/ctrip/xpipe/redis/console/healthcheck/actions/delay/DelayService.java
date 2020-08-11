package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;

import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
public interface DelayService {

    long getDelay(HostPort hostPort);

    long getDelay(ClusterType clusterType, HostPort hostPort);

    long getLocalCachedDelay(HostPort hostPort);

    Map<HostPort, Long> getDcCachedDelay(String dc);

    UnhealthyInfoModel getDcActiveClusterUnhealthyInstance(String dc);

    UnhealthyInfoModel getAllUnhealthyInstance();

    UnhealthyInfoModel getAllUnhealthyInstanceFromParallelService();
}
