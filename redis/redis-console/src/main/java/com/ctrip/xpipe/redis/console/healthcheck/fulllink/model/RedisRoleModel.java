package com.ctrip.xpipe.redis.console.healthcheck.fulllink.model;

import com.ctrip.xpipe.redis.core.protocal.pojo.AbstractRole;

public class RedisRoleModel {

    private String host;

    private int port;

    private AbstractRole role;

    private Throwable err;

    public RedisRoleModel() {
    }

    public RedisRoleModel(String host, int port, AbstractRole role) {
        this.host = host;
        this.port = port;
        this.role = role;
    }

    public RedisRoleModel(String host, int port, Throwable err) {
        this.host = host;
        this.port = port;
        this.err = err;
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

    public AbstractRole getRole() {
        return role;
    }

    public void setRole(AbstractRole role) {
        this.role = role;
    }

    public Throwable getErr() {
        return err;
    }

    public void setErr(Throwable err) {
        this.err = err;
    }
}
