package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterInstanceInfo;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author lishanglin
 * date 2021/1/14
 */
public class DefaultClusterInstanceInfo extends AbstractCheckInfo implements ClusterInstanceInfo {

    private int orgId;

    private DcGroupType type;

    public DefaultClusterInstanceInfo(String clusterId, String activeDc, ClusterType clusterType, int orgId) {
        super(clusterId, activeDc, clusterType);
        this.orgId = orgId;
    }

    @Override
    public ClusterInstanceInfo setOrgId(int orgId) {
        this.orgId = orgId;
        return this;
    }

    public int getOrgId() {
        return orgId;
    }

    @Override
    public DcGroupType getDcGroupType() {
        return type;
    }

    @Override
    public ClusterInstanceInfo setDcGroupType(DcGroupType type) {
        this.type = type;
        return this;
    }

    @Override
    public String toString() {
        return StringUtil.join(", ", clusterId, activeDc, clusterType, orgId);
    }

}
