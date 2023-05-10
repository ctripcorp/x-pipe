package com.ctrip.xpipe.redis.core.proxy.monitor;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.utils.DateTimeUtils;

import java.io.Serializable;
import java.util.Objects;

public class TunnelStatsResult implements Serializable {

    public static final long NOT_CLOSE = -1L;

    private String tunnelId;

    private String tunnelState;

    private HostPort frontend;

    private HostPort backend;

    private long protocolRecvTime;

    private long protocolSndTime;

    private long closeTime;

    private String closeFrom;

    public TunnelStatsResult() {
    }

    public TunnelStatsResult(String tunnelId, String tunnelState, long protocolRecvTime, long protocolSndTime, HostPort frontend, HostPort backend) {
        this(tunnelId, tunnelState, frontend, backend, protocolRecvTime, protocolSndTime, NOT_CLOSE, null);
    }

    public TunnelStatsResult(String tunnelId, String tunnelState, HostPort frontend, HostPort backend, long protocolRecvTime, long protocolSndTime,
                             long closeTime, String closeFrom) {
        this.tunnelId = tunnelId;
        this.tunnelState = tunnelState;
        this.frontend = frontend;
        this.backend = backend;
        this.protocolRecvTime = protocolRecvTime;
        this.protocolSndTime = protocolSndTime;
        this.closeTime = closeTime;
        this.closeFrom = closeFrom == null ? "Not Yet" : closeFrom;
    }

    public Object toArrayObject() {
        Object[] result = new Object[8];
        result[0] = tunnelId;
        result[1] = tunnelState;
        result[2] = protocolRecvTime;
        result[3] = protocolSndTime;
        result[4] = closeTime;
        result[5] = closeFrom;
        result[6] = frontend.toString();
        result[7] = backend.toString();
        return result;
    }

    public static TunnelStatsResult parse(Object object) {
        Object[] sample = (Object[]) object;
        String tunnelId = (String) sample[0];
        String tunnelState = (String) sample[1];
        long protocolRecvTime = (long) sample[2];
        long protocolSndTime = (long) sample[3];
        long closeTime = (long) sample[4];
        String closeFrom = (String) sample[5];
        HostPort frontend = HostPort.fromString((String) sample[6]);
        HostPort backend = HostPort.fromString((String) sample[7]);
        return new TunnelStatsResult(tunnelId, tunnelState, frontend, backend, protocolRecvTime, protocolSndTime, closeTime, closeFrom);
    }

    public String getTunnelId() {
        return tunnelId;
    }

    public String getTunnelState() {
        return tunnelState;
    }

    public long getProtocolRecvTime() {
        return protocolRecvTime;
    }

    public long getProtocolSndTime() {
        return protocolSndTime;
    }

    public long getCloseTime() {
        return closeTime;
    }

    public String getCloseFrom() {
        return closeFrom;
    }

    public HostPort getFrontend() {
        return frontend;
    }

    public HostPort getBackend() {
        return backend;
    }

    public TunnelStatsResult setTunnelId(String tunnelId) {
        this.tunnelId = tunnelId;
        return this;
    }

    public TunnelStatsResult setTunnelState(String tunnelState) {
        this.tunnelState = tunnelState;
        return this;
    }

    public TunnelStatsResult setFrontend(HostPort frontend) {
        this.frontend = frontend;
        return this;
    }

    public TunnelStatsResult setBackend(HostPort backend) {
        this.backend = backend;
        return this;
    }

    public TunnelStatsResult setProtocolRecvTime(long protocolRecvTime) {
        this.protocolRecvTime = protocolRecvTime;
        return this;
    }

    public TunnelStatsResult setProtocolSndTime(long protocolSndTime) {
        this.protocolSndTime = protocolSndTime;
        return this;
    }

    public TunnelStatsResult setCloseTime(long closeTime) {
        this.closeTime = closeTime;
        return this;
    }

    public TunnelStatsResult setCloseFrom(String closeFrom) {
        this.closeFrom = closeFrom;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TunnelStatsResult that = (TunnelStatsResult) o;
        return protocolRecvTime == that.protocolRecvTime &&
                protocolSndTime == that.protocolSndTime &&
                closeTime == that.closeTime &&
                Objects.equals(tunnelId, that.tunnelId) &&
                Objects.equals(tunnelState, that.tunnelState) &&
                Objects.equals(closeFrom, that.closeFrom);
    }

    @Override
    public int hashCode() {

        return Objects.hash(tunnelId, tunnelState, protocolRecvTime, protocolSndTime, closeTime, closeFrom);
    }

    @Override
    public String toString() {
        return "TunnelStatsResult{" +
                "tunnelId='" + tunnelId + '\'' +
                ", tunnelState='" + tunnelState + '\'' +
                ", protocolRecvTime=" + DateTimeUtils.timeAsString(protocolRecvTime) +
                ", protocolSndTime=" + DateTimeUtils.timeAsString(protocolSndTime) +
                ", closeTime=" + DateTimeUtils.timeAsString(closeTime) +
                ", closeFrom='" + closeFrom + '\'' +
                '}';
    }
}
