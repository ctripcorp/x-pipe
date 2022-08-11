package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.utils.StringUtil;

public class ClusterExchangeNameInfo extends AbstractCreateInfo{

    private Long formerClusterId;

    private String formerClusterName;

    private Long latterClusterId;

    private String latterClusterName;

    private String token;

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

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    @Override
    public void check() throws CheckFailException{

        if(StringUtil.isEmpty(formerClusterName)){
            throw new CheckFailException("formerClusterName empty");
        }

        if(StringUtil.isEmpty(latterClusterName)){
            throw new CheckFailException("latterClusterName empty");
        }

        if(StringUtil.isEmpty(token)){
            throw new CheckFailException("token empty");
        }

    }

    @Override
    public String toString() {
        return JsonCodec.INSTANCE.encode(this);
    }
}
