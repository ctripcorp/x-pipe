package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;

/**
 * @author chen.zhu
 * <p>
 * Aug 23, 2018
 */
public interface RedisInstanceInfo {

    ClusterShardHostPort getClusterShardHostport();

    String getClusterId();

    String getShardId();

    String getDcId();

    HostPort getHostPort();

    boolean isMaster();

    void isMaster(boolean master);

    String getActiveDc();

    void setActiveDc(String activeDc);

    boolean isInActiveDc();

    boolean isReplThroughProxy();
}
