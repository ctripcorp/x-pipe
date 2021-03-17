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

    private HostPort proxyHost;

    public HostPort getFrontend() {
        return frontend;
    }

    public TunnelStatsInfo setFrontend(HostPort frontend) {
        this.frontend = frontend;
        return this;
    }

    public HostPort getBackend() {
        return backend;
    }

    public TunnelStatsInfo setBackend(HostPort backend) {
        this.backend = backend;
        return this;
    }

    public HostPort getProxyHost() {
        return proxyHost;
    }

    public TunnelStatsInfo setProxyHost(HostPort proxyHost) {
        this.proxyHost = proxyHost;
        return this;
    }

    public String getTunnelId() {
        return tunnelId;
    }

    public TunnelStatsInfo setTunnelId(String tunnelId) {
        this.tunnelId = tunnelId;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TunnelStatsInfo that = (TunnelStatsInfo) o;
        return Objects.equals(tunnelId, that.tunnelId) &&
                Objects.equals(frontend, that.frontend) &&
                Objects.equals(backend, that.backend) &&
                Objects.equals(proxyHost, that.proxyHost);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tunnelId, frontend, backend, proxyHost);
    }

    @Override
    public String toString() {
        return "TunnelStatsInfo{" +
                "tunnelId='" + tunnelId + '\'' +
                ", frontend=" + frontend +
                ", backend=" + backend +
                ", proxyHost=" + proxyHost +
                '}';
    }
}
