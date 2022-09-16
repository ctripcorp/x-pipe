package com.ctrip.xpipe.redis.checker.healthcheck;

import com.ctrip.xpipe.cluster.DcGroupType;

/**
 * @author lishanglin
 * date 2021/1/10
 */
public interface ClusterInstanceInfo extends CheckInfo {

    ClusterInstanceInfo setOrgId(int orgId);

    int getOrgId();

    DcGroupType getDcGroupType();

    ClusterInstanceInfo setDcGroupType(DcGroupType type);

}
