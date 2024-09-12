package com.ctrip.xpipe.redis.console.healthcheck.fulllink.model;

import com.ctrip.xpipe.redis.checker.model.CheckerRole;

import java.util.ArrayList;
import java.util.List;

public class ShardCheckerHealthCheckModel {

    private String host;
    private int port;
    private CheckerRole checkerRole;
    private List<InstanceCheckerHealthCheckModel> instances;

    public ShardCheckerHealthCheckModel() {
    }

    public ShardCheckerHealthCheckModel(String host, int port) {
        this.host = host;
        this.port = port;
        this.checkerRole = CheckerRole.FOLLOWER;
        this.instances = new ArrayList<>();
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public CheckerRole getCheckerRole() {
        return checkerRole;
    }

    public void setCheckerRole(CheckerRole checkerRole) {
        this.checkerRole = checkerRole;
    }

    public List<InstanceCheckerHealthCheckModel> getInstances() {
        return instances;
    }

    public void setInstances(List<InstanceCheckerHealthCheckModel> instances) {
        this.instances = instances;
    }

}
