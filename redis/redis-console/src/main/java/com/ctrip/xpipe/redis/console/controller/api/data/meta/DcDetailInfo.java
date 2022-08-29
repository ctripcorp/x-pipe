package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.codec.JsonCodec;

/**
 * @author: cchen6
 * 2022/8/29
 */
public class DcDetailInfo {
    private String dcId;
    private Boolean dcGroupType;
    private String dcGroupName;

    public String getDcId() {
        return dcId;
    }

    public DcDetailInfo setDcId(String dcId) {
        this.dcId = dcId;
        return this;
    }

    public Boolean getDcGroupType() {
        return dcGroupType;
    }

    public DcDetailInfo setDcGroupType(Boolean dcGroupType) {
        this.dcGroupType = dcGroupType;
        return this;
    }

    public String getDcGroupName() {
        return dcGroupName;
    }

    public DcDetailInfo setDcGroupName(String dcGroupName) {
        this.dcGroupName = dcGroupName;
        return this;
    }

    @Override
    public String toString() {
        return JsonCodec.INSTANCE.encode(this);
    }
}
