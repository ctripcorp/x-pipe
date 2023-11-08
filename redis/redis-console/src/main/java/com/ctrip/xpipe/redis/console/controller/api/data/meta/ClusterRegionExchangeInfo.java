package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.utils.StringUtil;

public class ClusterRegionExchangeInfo extends AbstractCreateInfo {

    private Long formerClusterId;

    private String formerClusterName;

    private Long latterClusterId;

    private String latterClusterName;

    private String regionName;

    public ClusterRegionExchangeInfo() {
    }

    public ClusterRegionExchangeInfo(Long formerClusterId, String formerClusterName, Long latterClusterId,
        String latterClusterName, String regionName) {
        this.formerClusterId = formerClusterId;
        this.formerClusterName = formerClusterName;
        this.latterClusterId = latterClusterId;
        this.latterClusterName = latterClusterName;
        this.regionName = regionName;
    }

    public Long getFormerClusterId() {
        return formerClusterId;
    }

    public void setFormerClusterId(Long formerClusterId) {
        this.formerClusterId = formerClusterId;
    }

    public String getFormerClusterName() {
        return formerClusterName;
    }

    public void setFormerClusterName(String formerClusterName) {
        this.formerClusterName = formerClusterName;
    }

    public Long getLatterClusterId() {
        return latterClusterId;
    }

    public void setLatterClusterId(Long latterClusterId) {
        this.latterClusterId = latterClusterId;
    }

    public String getLatterClusterName() {
        return latterClusterName;
    }

    public void setLatterClusterName(String latterClusterName) {
        this.latterClusterName = latterClusterName;
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    @Override
    public void check() throws CheckFailException {
        if(StringUtil.isEmpty(formerClusterName)){
            throw new CheckFailException("formerClusterName empty");
        }

        if(StringUtil.isEmpty(latterClusterName)){
            throw new CheckFailException("latterClusterName empty");
        }

        if (StringUtil.isEmpty(regionName)) {
            throw new CheckFailException("regionName empty");
        }
    }

    @Override
    public String toString() {
        return JsonCodec.INSTANCE.encode(this);
    }
}
