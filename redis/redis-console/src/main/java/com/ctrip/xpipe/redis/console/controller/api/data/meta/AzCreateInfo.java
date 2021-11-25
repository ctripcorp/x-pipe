package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author: Song_Yu
 * @date: 2021/11/8
 */
public class AzCreateInfo extends AbstractCreateInfo{
    private String dcName;

    private Boolean active;

    private String azName;

    private String description;

    @Override
    public void check() throws CheckFailException {
        if(StringUtil.isEmpty(dcName)){
            throw new CheckFailException("dcName empty");
        }

        if(StringUtil.isEmpty(azName)){
            throw new CheckFailException("azName empty");
        }

    }

    public String getDcName() {
        return dcName;
    }

    public AzCreateInfo setDcName(String dcName) {
        this.dcName = dcName;
        return this;
    }

    public Boolean isActive() {
        return active;
    }

    public AzCreateInfo setActive(Boolean active) {
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
        return description;
    }

    public AzCreateInfo setDescription(String description) {
        this.description = description;
        return this;
    }

    @Override
    public String toString() {
        return "AzCreateInfo{" +
                "dcName='" + dcName + '\'' +
                ", active=" + active + '\'' +
                ", azName='" + azName + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
