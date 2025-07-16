package com.ctrip.xpipe.redis.checker.healthcheck;

/**
 * @author lishanglin
 * date 2021/1/10
 */
public interface ClusterInstanceInfo extends CheckInfo {

    ClusterInstanceInfo setOrgId(int orgId);

    int getOrgId();

    ClusterInstanceInfo setDcId(String dcId);

    String getDcId();

}
