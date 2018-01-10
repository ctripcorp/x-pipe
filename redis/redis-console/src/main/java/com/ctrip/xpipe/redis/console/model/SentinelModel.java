package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.endpoint.HostPort;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jan 05, 2018
 */
public class SentinelModel {

    private String dcName;

    private List<HostPort> sentinels;

    private String desc;

    public String getDcName() {
        return dcName;
    }

    public SentinelModel setDcName(String dcName) {
        this.dcName = dcName;
        return this;
    }

    public List<HostPort> getSentinels() {
        return sentinels;
    }

    public SentinelModel setSentinels(List<HostPort> sentinels) {
        this.sentinels = sentinels;
        return this;
    }

    public String getDesc() {
        return desc;
    }

    public SentinelModel setDesc(String desc) {
        this.desc = desc;
        return this;
    }
}
