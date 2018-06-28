package com.ctrip.xpipe.redis.console.model;

/**
 * @author chen.zhu
 * <p>
 * Jun 19, 2018
 */
public class RouteModel {

    private long id;

    private long orgId;

    private String srcProxyIds;

    private String dstProxyIds;

    private String optionProxyIds;

    private String srcDcName;

    private String dstDcName;

    private String tag;

    private boolean active;

    public long getId() {
        return id;
    }

    public RouteModel setId(long id) {
        this.id = id;
        return this;
    }

    public long getOrgId() {
        return orgId;
    }

    public RouteModel setOrgId(long orgId) {
        this.orgId = orgId;
        return this;
    }

    public String getSrcProxyIds() {
        return srcProxyIds;
    }

    public RouteModel setSrcProxyIds(String srcProxyIds) {
        this.srcProxyIds = srcProxyIds;
        return this;
    }

    public String getDstProxyIds() {
        return dstProxyIds;
    }

    public RouteModel setDstProxyIds(String dstProxyIds) {
        this.dstProxyIds = dstProxyIds;
        return this;
    }

    public String getOptionProxyIds() {
        return optionProxyIds;
    }

    public RouteModel setOptionProxyIds(String optionProxyIds) {
        this.optionProxyIds = optionProxyIds;
        return this;
    }

    public String getSrcDcName() {
        return srcDcName;
    }

    public RouteModel setSrcDcName(String srcDcName) {
        this.srcDcName = srcDcName;
        return this;
    }

    public String getDstDcName() {
        return dstDcName;
    }

    public RouteModel setDstDcName(String dstDcName) {
        this.dstDcName = dstDcName;
        return this;
    }

    public String getTag() {
        return tag;
    }

    public RouteModel setTag(String tag) {
        this.tag = tag;
        return this;
    }

    public boolean isActive() {
        return active;
    }

    public RouteModel setActive(boolean active) {
        this.active = active;
        return this;
    }
}
