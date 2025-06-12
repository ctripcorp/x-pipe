package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

/**
 * @author chen.zhu
 * <p>
 * Jan 05, 2018
 */
public class SentinelModel {

    private long id;

    private String dcName;

    private List<HostPort> sentinels;

    private String desc;

    public SentinelModel() {}

    public SentinelModel(SetinelTbl setinelTbl) {
        this.id = setinelTbl.getSetinelId();
        this.dcName = setinelTbl.getDcName();
        this.desc = setinelTbl.getSetinelDescription();
        List<HostPort> sentinels = Lists.transform(
                Lists.newArrayList(StringUtil.splitRemoveEmpty(",", setinelTbl.getSetinelAddress())),
                new Function<String, HostPort>() {
                    @Override
                    public HostPort apply(String input) {
                        return HostPort.fromString(input);
                    }
                });
        this.sentinels = Lists.newArrayList(sentinels);
    }

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

    public long getId() {
        return id;
    }

    public SentinelModel setId(long id) {
        this.id = id;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SentinelModel that = (SentinelModel) o;

        boolean result = id == that.id &&
                Objects.equals(dcName, that.dcName) &&
                Objects.equals(desc, that.desc);
        if(!result) {
            return false;
        }
        if(sentinels == null && that.sentinels == null) {
            return true;
        }

        if(sentinels == null || that.sentinels == null) {
            return false;
        }
        if(sentinels.size() != that.sentinels.size()) {
            return false;
        }
        for(int i = 0; i < sentinels.size(); i++) {
            if(!sentinels.get(i).equals(that.sentinels.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, dcName, sentinels, desc);
    }
}
