package com.ctrip.xpipe.redis.checker.model;

import com.ctrip.xpipe.endpoint.HostPort;

import java.util.Objects;

/**
 * @author lishanglin
 * date 2021/3/11
 */
public class TunnelStatsInfo {

    private String tunnelId;

    private HostPort frontend;

    private HostPort backend;

    public HostPort getFrontend() {
        return frontend;
    }

    public void setFrontend(HostPort frontend) {
        this.frontend = frontend;
    }

    public HostPort getBackend() {
        return backend;
    }

    public void setBackend(HostPort backend) {
        this.backend = backend;
    }

    public String getTunnelId() {
        return tunnelId;
    }

    public void setTunnelId(String tunnelId) {
        this.tunnelId = tunnelId;
    }

    @Override
    public String toString() {
        return "TunnelStatsInfo{" +
                "tunnelId='" + tunnelId + '\'' +
                ", frontend=" + frontend +
                ", backend=" + backend +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TunnelStatsInfo that = (TunnelStatsInfo) o;
        return Objects.equals(tunnelId, that.tunnelId) &&
                Objects.equals(frontend, that.frontend) &&
                Objects.equals(backend, that.backend);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tunnelId, frontend, backend);
    }

}
