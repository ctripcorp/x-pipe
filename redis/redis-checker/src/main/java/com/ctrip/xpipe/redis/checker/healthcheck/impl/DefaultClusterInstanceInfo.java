package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterInstanceInfo;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/1/14
 */
public class DefaultClusterInstanceInfo extends AbstractCheckInfo implements ClusterInstanceInfo {

    private int orgId;
    private String lastModifyTime;
    private List<String> dcs;

    public DefaultClusterInstanceInfo(String clusterId, String activeDc, ClusterType clusterType, int orgId, String lastModifyTime) {
        super(clusterId, activeDc, clusterType);
        this.orgId = orgId;
        this.clusterOrgId = orgId;
        this.lastModifyTime = lastModifyTime;
    }

    @Override
    public ClusterInstanceInfo setOrgId(int orgId) {
        this.orgId = orgId;
        this.clusterOrgId = orgId;
        return this;
    }

    public int getOrgId() {
        return orgId;
    }

    public String getLastModifyTime() {
        return lastModifyTime;
    }

    @Override
    public ClusterInstanceInfo setDcs(List<String> dcs) {
        this.dcs = dcs;
        return this;
    }

    @Override
    public List<String> getDcs() {
        return dcs;
    }

    @Override
    public String toString() {
        return StringUtil.join(", ", clusterId, activeDc, clusterType, orgId, "azGroupType:" + azGroupType,
            "isAsymmetricCluster:" + asymmetricCluster, "dcs:" + dcs);
    }

}
