package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Objects;

/**
 * @author chen.zhu
 * <p>
 * Sep 20, 2018
 */
public class DcClusterDelayMarkDown {

    private String dcId;

    private String clusterId;

    @JsonIgnore
    private int delaySecond;

    public String getDcId() {
        return dcId;
    }

    public DcClusterDelayMarkDown setDcId(String dcId) {
        this.dcId = dcId;
        return this;
    }

    public String getClusterId() {
        return clusterId;
    }

    public DcClusterDelayMarkDown setClusterId(String clusterId) {
        this.clusterId = clusterId;
        return this;
    }

    public int getDelaySecond() {
        return delaySecond;
    }

    public DcClusterDelayMarkDown setDelaySecond(int delaySecond) {
        this.delaySecond = delaySecond;
        return this;
    }

    public boolean matches(String dcId, String clusterId) {
        return this.dcId.equals(dcId) && this.clusterId.equals(clusterId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DcClusterDelayMarkDown that = (DcClusterDelayMarkDown) o;
        return delaySecond == that.delaySecond &&
                Objects.equals(dcId, that.dcId) &&
                Objects.equals(clusterId, that.clusterId);
    }

    @Override
    public int hashCode() {

        return Objects.hash(dcId, clusterId, delaySecond);
    }
}
