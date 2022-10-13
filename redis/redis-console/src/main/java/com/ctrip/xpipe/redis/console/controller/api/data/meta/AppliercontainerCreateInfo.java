package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.utils.IpUtils;
import org.springframework.lang.Nullable;

public class AppliercontainerCreateInfo extends AbstractCreateInfo{

    private long id;

    private String dcName;

    private String appliercontainerIp;

    private int appliercontainerPort;

    private long appliercontainerOrgId;

    @Nullable
    private String orgName;

    private boolean active;

    private String azName;

    public AppliercontainerCreateInfo() {
    }

    @Override
    public void check() throws CheckFailException {
        if (!IpUtils.isValidIpFormat(appliercontainerIp)) {
            throw new CheckFailException("Illegal IP Address");
        }
        if (appliercontainerPort == 0) {
            throw new CheckFailException("Illegal Port");
        }
    }

    public String getDcName() {
        return dcName;
    }

    public AppliercontainerCreateInfo setDcName(String dcName) {
        this.dcName = dcName;
        return this;
    }

    public String getAppliercontainerIp() {
        return appliercontainerIp;
    }

    public AppliercontainerCreateInfo setAppliercontainerIp(String appliercontainerIp) {
        this.appliercontainerIp = appliercontainerIp;
        return this;
    }

    public int getAppliercontainerPort() {
        return appliercontainerPort;
    }

    public AppliercontainerCreateInfo setAppliercontainerPort(int appliercontainerPort) {
        this.appliercontainerPort = appliercontainerPort;
        return this;
    }

    public long getAppliercontainerOrgId() {
        return appliercontainerOrgId;
    }

    public AppliercontainerCreateInfo setAppliercontainerOrgId(long appliercontainerOrgId) {
        this.appliercontainerOrgId = appliercontainerOrgId;
        return this;
    }

    @Nullable
    public String getOrgName() {
        return orgName;
    }

    public AppliercontainerCreateInfo setOrgName(@Nullable String orgName) {
        this.orgName = orgName;
        return this;
    }

    public boolean isActive() {
        return active;
    }

    public AppliercontainerCreateInfo setActive(boolean active) {
        this.active = active;
        return this;
    }

    public String getAzName() {
        return azName;
    }

    public AppliercontainerCreateInfo setAzName(String azName) {
        this.azName = azName;
        return this;
    }

    public long getId() {
        return id;
    }

    public AppliercontainerCreateInfo setId(long id) {
        this.id = id;
        return this;
    }

    @Override
    public String toString() {
        return "AppliercontainerCreateInfo{" +
                "id=" + id +
                ", dcName='" + dcName + '\'' +
                ", appliercontainerIp='" + appliercontainerIp + '\'' +
                ", appliercontainerPort=" + appliercontainerPort +
                ", appliercontainerOrgId=" + appliercontainerOrgId +
                ", orgName='" + orgName + '\'' +
                ", active=" + active +
                ", azName='" + azName + '\'' +
                '}';
    }
}
