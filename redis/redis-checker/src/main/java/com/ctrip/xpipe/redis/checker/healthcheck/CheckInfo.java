package com.ctrip.xpipe.redis.checker.healthcheck;

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
