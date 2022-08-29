package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author: cchen6
 * 2022/8/29
 */
public class ReplDirectionCreateInfo extends AbstractCreateInfo {
    private String srcDcName;
    private String fromDcName;
    private String toDcName;
    private String targetClusterName;

    @Override
    public void check() throws CheckFailException {
        if(StringUtil.isEmpty(srcDcName)) {
            throw new CheckFailException("srcDcName empty");
        }
        if(StringUtil.isEmpty(fromDcName)) {
            throw new CheckFailException("fromDcName empty");
        }
        if(StringUtil.isEmpty(toDcName)) {
            throw new CheckFailException("toDcName empty");
        }
    }

    public String getSrcDcName() {
        return srcDcName;
    }

    public ReplDirectionCreateInfo setSrcDcName(String srcDcName) {
        this.srcDcName = srcDcName;
        return this;
    }

    public String getFromDcName() {
        return fromDcName;
    }

    public ReplDirectionCreateInfo setFromDcName(String fromDcName) {
        this.fromDcName = fromDcName;
        return this;
    }

    public String getToDcName() {
        return toDcName;
    }

    public ReplDirectionCreateInfo setToDcName(String toDcName) {
        this.toDcName = toDcName;
        return this;
    }

    public String getTargetClusterName() {
        return targetClusterName;
    }

    public ReplDirectionCreateInfo setTargetClusterName(String targetClusterName) {
        this.targetClusterName = targetClusterName;
        return this;
    }

    @Override
    public String toString() {
        return JsonCodec.INSTANCE.encode(this);
    }
}
