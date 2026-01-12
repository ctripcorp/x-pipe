package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import java.util.Objects;

public class InstanceInfo {
    private String ip;
    private int port;
    private String dc;

    public InstanceInfo() {
    }

    public InstanceInfo(String ip, int port, String dc) {
        this.ip = ip;
        this.dc = dc;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDc() {
        return dc;
    }

    public void setDc(String dc) {
        this.dc = dc;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        InstanceInfo that = (InstanceInfo) o;
        return port == that.port && Objects.equals(ip, that.ip) && Objects.equals(dc, that.dc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, port, dc);
    }
}
