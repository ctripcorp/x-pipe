package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.healthcheck.ClusterInstanceInfo;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author lishanglin
 * date 2021/1/14
 */
public class DefaultClusterInstanceInfo extends AbstractCheckInfo implements ClusterInstanceInfo {

    private int orgId;

    public DefaultClusterInstanceInfo(String clusterId, String activeDc, ClusterType clusterType, int orgId) {
        super(clusterId, activeDc, clusterType);
        this.orgId = orgId;
    }

    public int getOrgId() {
        return orgId;
    }

    @Override
    public String toString() {
        return StringUtil.join(", ", clusterId, orgId);
    }

}
