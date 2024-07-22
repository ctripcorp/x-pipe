package com.ctrip.xpipe.redis.console.healthcheck.fulllink.model;

import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;


public class InstanceCheckerHealthCheckModel {

    private String host;
    private int port;
    private String actions;
    private HEALTH_STATE state;

    public InstanceCheckerHealthCheckModel() {
    }

    public String getHost() {
        return host;
    }

    public InstanceCheckerHealthCheckModel setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public InstanceCheckerHealthCheckModel setPort(int port) {
        this.port = port;
        return this;
    }

    public String getActions() {
        return actions;
    }

    public InstanceCheckerHealthCheckModel setActions(String actions) {
        this.actions = actions;
        return this;
    }

    public HEALTH_STATE getState() {
        return state;
    }

    public InstanceCheckerHealthCheckModel setState(HEALTH_STATE state) {
        this.state = state;
        return this;
    }
}
