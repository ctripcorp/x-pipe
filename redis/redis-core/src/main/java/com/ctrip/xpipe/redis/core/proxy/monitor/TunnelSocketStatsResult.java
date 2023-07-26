package com.ctrip.xpipe.redis.core.proxy.monitor;


import com.ctrip.xpipe.exception.XpipeRuntimeException;

import java.io.Serializable;
import java.util.Objects;

public class TunnelSocketStatsResult implements Serializable {

    private String tunnelId;

    private SocketStatsResult frontendSocketStats;

    private SocketStatsResult backendSocketStats;

    public TunnelSocketStatsResult() {
    }

    public TunnelSocketStatsResult(String tunnelId, SocketStatsResult frontendSocketStats,
                                   SocketStatsResult backendSocketStats) {
        this.tunnelId = tunnelId;
        this.frontendSocketStats = frontendSocketStats;
        this.backendSocketStats = backendSocketStats;
    }

    public Object format() {
        Object[] frontendResult = frontendSocketStats.toArray();
        Object[] backendResult = backendSocketStats.toArray();
        Object[] result = new Object[3];
        result[0] = tunnelId;
        result[1] = frontendResult;
        result[2] = backendResult;
        return result;
    }

    public static TunnelSocketStatsResult parse(Object obj) {
        if(!obj.getClass().isArray()) {
            throw new XpipeRuntimeException("Illegal TunnelSocketStatsResult meta data, should be an array");
        }
        Object[] metaData = (Object[]) obj;
        if(!(metaData[0] instanceof String)) {
            throw new XpipeRuntimeException("Illegal TunnelSocketStatsResult meta data, first element should be string");
        }
        Object[] frontMeta = (Object[]) metaData[1];
        Object[] backMeta = (Object[]) metaData[2];
        return new TunnelSocketStatsResult((String)metaData[0],
                SocketStatsResult.parseFromArray(frontMeta),
                SocketStatsResult.parseFromArray(backMeta));
    }

    public String getTunnelId() {
        return tunnelId;
    }

    public SocketStatsResult getFrontendSocketStats() {
        return frontendSocketStats;
    }

    public SocketStatsResult getBackendSocketStats() {
        return backendSocketStats;
    }

    public TunnelSocketStatsResult setTunnelId(String tunnelId) {
        this.tunnelId = tunnelId;
        return this;
    }

    public TunnelSocketStatsResult setFrontendSocketStats(SocketStatsResult frontendSocketStats) {
        this.frontendSocketStats = frontendSocketStats;
        return this;
    }

    public TunnelSocketStatsResult setBackendSocketStats(SocketStatsResult backendSocketStats) {
        this.backendSocketStats = backendSocketStats;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TunnelSocketStatsResult that = (TunnelSocketStatsResult) o;
        return Objects.equals(tunnelId, that.tunnelId) &&
                Objects.equals(frontendSocketStats, that.frontendSocketStats) &&
                Objects.equals(backendSocketStats, that.backendSocketStats);
    }

    @Override
    public int hashCode() {

        return Objects.hash(tunnelId, frontendSocketStats, backendSocketStats);
    }

    @Override
    public String toString() {
        return "TunnelSocketStatsResult{" +
                "tunnelId='" + tunnelId + '\'' +
                ", frontendSocketStats=" + frontendSocketStats.toString() +
                ", backendSocketStats=" + backendSocketStats.toString() +
                '}';
    }
}
