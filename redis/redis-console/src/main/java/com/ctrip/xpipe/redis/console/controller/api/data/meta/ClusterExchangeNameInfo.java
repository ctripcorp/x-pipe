package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.LinkedList;
import java.util.List;

public class ClusterExchangeNameInfo extends AbstractCreateInfo{

    private Long formerClusterId;

    private String formerClusterName;

    private Long latterClusterId;

    private String latterClusterName;

    public ClusterExchangeNameInfo(){
    }

    public Long getFormerClusterId() {
        return formerClusterId;
    }

    public void setFormerClusterId(Long clusterId) {
        this.formerClusterId = clusterId;
    }

    public String getFormerClusterName() {
        return formerClusterName;
    }

    public void setFormerClusterName(String clusterName) {
        this.formerClusterName = clusterName;
    }

    public Long getLatterClusterId() {
        return latterClusterId;
    }

    public void setLatterClusterId(Long clusterId) {
        this.latterClusterId = clusterId;
    }

    public String getLatterClusterName() {
        return latterClusterName;
    }

    public void setLatterClusterName(String clusterName) {
        this.latterClusterName = clusterName;
    }

    @Override
    public void check() throws CheckFailException{

        if(StringUtil.isEmpty(formerClusterName)){
            throw new CheckFailException("formerClusterName empty");
        }

        if(StringUtil.isEmpty(latterClusterName)){
            throw new CheckFailException("latterClusterName empty");
        }
    }

    @Override
    public String toString() {
        return JsonCodec.INSTANCE.encode(this);
    }
}
