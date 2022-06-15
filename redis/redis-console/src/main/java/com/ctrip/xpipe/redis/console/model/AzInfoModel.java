package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.codec.JsonCodec;

import java.io.Serializable;

public class AzInfoModel implements Serializable {

    private static final long serialVersionUID = 1L;

    private String dcName;

    private Boolean active;

    private String azName;

    private String description;

    public AzInfoModel() {
    }

    public String getDcName() {
        return dcName;
    }

    public AzInfoModel setDcName(String dcName) {
        this.dcName = dcName;
        return this;
    }

    public Boolean getActive() {
        return active;
    }

    public AzInfoModel setActive(Boolean active) {
        this.active = active;
        return this;
    }

    public String getAzName() {
        return azName;
    }

    public AzInfoModel setAzName(String azName) {
        this.azName = azName;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public AzInfoModel setDescription(String description) {
        this.description = description;
        return this;
    }

    @Override
    public String toString() {
        return JsonCodec.INSTANCE.encode(this);
    }
}
