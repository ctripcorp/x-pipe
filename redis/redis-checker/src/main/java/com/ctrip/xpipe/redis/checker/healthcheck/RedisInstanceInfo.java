package com.ctrip.xpipe.redis.checker.healthcheck;

import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Aug 23, 2018
 */
public interface RedisInstanceInfo extends CheckInfo {

    ClusterShardHostPort getClusterShardHostport();

    String getShardId();

    String getDcId();

    HostPort getHostPort();

    boolean isMaster();

    void isMaster(boolean master);

    boolean isInActiveDc();

    boolean isCrossRegion();

    Long getShardDbId();

    List<Long> getActiveDcAllShardIds();

}
