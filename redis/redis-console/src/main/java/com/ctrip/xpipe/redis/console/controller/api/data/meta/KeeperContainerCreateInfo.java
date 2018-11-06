package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.utils.IpUtils;
import jline.internal.Nullable;

/**
 * @author chen.zhu
 * <p>
 * Mar 07, 2018
 */
public class KeeperContainerCreateInfo extends AbstractCreateInfo {

    private String dcName;

    private String keepercontainerIp;

    private int keepercontainerPort;

    private long keepercontainerOrgId;

    @Nullable
    private String orgName;

    private boolean active;

    public boolean isActive() {
        return active;
    }

    public KeeperContainerCreateInfo setActive(boolean active) {
        this.active = active;
        return this;
    }

    @Override
    public void check() throws CheckFailException {
        if(!IpUtils.isValidIpFormat(keepercontainerIp)) {
            throw new CheckFailException("Illegal IP Address");
        }
        if(keepercontainerPort == 0) {
            throw new CheckFailException("Illegal Port");
        }
    }

    public String getDcName() {
        return dcName;
    }

    public KeeperContainerCreateInfo setDcName(String dcName) {
        this.dcName = dcName;
        return this;
    }

    public String getKeepercontainerIp() {
        return keepercontainerIp;
    }

    public KeeperContainerCreateInfo setKeepercontainerIp(String keepercontainerIp) {
        this.keepercontainerIp = keepercontainerIp;
        return this;
    }

    public int getKeepercontainerPort() {
        return keepercontainerPort;
    }

    public KeeperContainerCreateInfo setKeepercontainerPort(int keepercontainerPort) {
        this.keepercontainerPort = keepercontainerPort;
        return this;
    }

    public long getKeepercontainerOrgId() {
        return keepercontainerOrgId;
    }

    public KeeperContainerCreateInfo setKeepercontainerOrgId(long keepercontainerOrgId) {
        this.keepercontainerOrgId = keepercontainerOrgId;
        return this;
    }

    @Override
    public String toString() {
        return "KeeperContainerCreateInfo{" +
                "dcName='" + dcName + '\'' +
                ", keepercontainerIp='" + keepercontainerIp + '\'' +
                ", keepercontainerPort=" + keepercontainerPort +
                ", keepercontainerOrgId=" + keepercontainerOrgId +
                ", orgName='" + orgName + '\'' +
                ", active=" + active +
                '}';
    }

    public String getOrgName() {
        return orgName;
    }

    public KeeperContainerCreateInfo setOrgName(String orgName) {
        this.orgName = orgName;
        return this;
    }
}
