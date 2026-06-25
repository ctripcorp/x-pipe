package com.ctrip.xpipe.redis.console.model.consoleportal;

import com.ctrip.xpipe.redis.console.model.DcTbl;

import java.util.ArrayList;
import java.util.List;

public class ClusterDcGroupModel {

    private Long azGroupId;
    private Long azGroupClusterId;
    private String region;
    private String azGroupClusterType;
    private Long activeAzId;
    private List<DcTbl> dcs = new ArrayList<>();

    public Long getAzGroupId() {
        return azGroupId;
    }

    public ClusterDcGroupModel setAzGroupId(Long azGroupId) {
        this.azGroupId = azGroupId;
        return this;
    }

    public Long getAzGroupClusterId() {
        return azGroupClusterId;
    }

    public ClusterDcGroupModel setAzGroupClusterId(Long azGroupClusterId) {
        this.azGroupClusterId = azGroupClusterId;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public ClusterDcGroupModel setRegion(String region) {
        this.region = region;
        return this;
    }

    public String getAzGroupClusterType() {
        return azGroupClusterType;
    }

    public ClusterDcGroupModel setAzGroupClusterType(String azGroupClusterType) {
        this.azGroupClusterType = azGroupClusterType;
        return this;
    }

    public Long getActiveAzId() {
        return activeAzId;
    }

    public ClusterDcGroupModel setActiveAzId(Long activeAzId) {
        this.activeAzId = activeAzId;
        return this;
    }

    public List<DcTbl> getDcs() {
        return dcs;
    }

    public ClusterDcGroupModel setDcs(List<DcTbl> dcs) {
        this.dcs = dcs;
        return this;
    }
}
