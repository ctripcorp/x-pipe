package com.ctrip.xpipe.redis.console.healthcheck;

/**
 * @author lishanglin
 * date 2021/1/10
 */
public interface ClusterInstanceInfo extends CheckInfo {

    ClusterInstanceInfo setOrgId(int orgId);

    int getOrgId();

}
