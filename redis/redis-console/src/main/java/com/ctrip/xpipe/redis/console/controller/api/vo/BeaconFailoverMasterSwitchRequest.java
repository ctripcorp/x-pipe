package com.ctrip.xpipe.redis.console.controller.api.vo;

public class BeaconFailoverMasterSwitchRequest {

    private String ip;

    private Integer port;

    public BeaconFailoverMasterSwitchRequest() {
    }

    public BeaconFailoverMasterSwitchRequest(String ip, Integer port) {
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }
}
