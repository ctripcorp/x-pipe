package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.cluster.ClusterType;

/**
 * @author lishanglin
 * date 2021/1/10
 */
public interface CheckInfo {

    String getClusterId();

    ClusterType getClusterType();

    String getActiveDc();

    void setActiveDc(String activeDc);

}
