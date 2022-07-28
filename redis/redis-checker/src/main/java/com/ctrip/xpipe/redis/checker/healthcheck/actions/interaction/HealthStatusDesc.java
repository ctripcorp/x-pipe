package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.endpoint.HostPort;

/**
 * @author lishanglin
 * date 2022/7/18
 */
public class HealthStatusDesc {

    private HostPort hostPort;

    private HEALTH_STATE state;

    private long lastPongTime;

    private long lastHealthDelayTime;

    public HealthStatusDesc() {

    }

    public HealthStatusDesc(HostPort hostPort, HealthStatus status) {
        this.hostPort = hostPort;
        this.state = status.getState();
        this.lastPongTime = status.getLastPongTime();
        this.lastHealthDelayTime = status.getLastHealthyDelayTime();
    }

    public HostPort getHostPort() {
        return hostPort;
    }

    public HEALTH_STATE getState() {
        return state;
    }

    public long getLastPongTime() {
        return lastPongTime;
    }

    public long getLastHealthDelayTime() {
        return lastHealthDelayTime;
    }

}
