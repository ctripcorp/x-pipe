package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author:
 * @date:
 */
public class AzCreateInfo extends AbstractCreateInfo{
    private String dcName;

    private boolean active;

    private String azName;

    private String desc;



    @Override
    public void check() throws CheckFailException {
        if(StringUtil.isEmpty(dcName)){
            throw new CheckFailException("dcName empty");
        }

        if(StringUtil.isEmpty(azName)){
            throw new CheckFailException("azName empty");
        }

        if(StringUtil.isEmpty(desc)){
            throw new CheckFailException("desc empty");
        }

    }

    public String getDcName() {
        return dcName;
    }

    public AzCreateInfo setDcName(String dcName) {
        this.dcName = dcName;
        return this;
    }

    public boolean isActive() {
        return active;
    }

    public AzCreateInfo setActive(boolean active) {
        this.active = active;
        return this;
    }

    public String getAzName() {
        return azName;
    }

    public AzCreateInfo setAzName(String azName) {
        this.azName = azName;
        return this;
    }

    public String getDescription() {
        return desc;
    }

    public AzCreateInfo setDescription(String description) {
        this.desc = description;
        return this;
    }

    @Override
    public String toString() {
        return "AzCreateInfo{" +
                "dcName='" + dcName + '\'' +
                ", active=" + active +
                ", azName='" + azName + '\'' +
                ", description='" + desc + '\'' +
                '}';
    }
}
