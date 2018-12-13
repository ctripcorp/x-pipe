package com.ctrip.xpipe.redis.console.model.consoleportal;

public class ProxyInfoModel {

    private String ip;

    private String dc;

    private int chains;

    public ProxyInfoModel(String ip, String dc, int chains) {
        this.ip = ip;
        this.dc = dc;
        this.chains = chains;
    }

    public String getIp() {
        return ip;
    }

    public ProxyInfoModel setIp(String ip) {
        this.ip = ip;
        return this;
    }

    public String getDc() {
        return dc;
    }

    public ProxyInfoModel setDc(String dc) {
        this.dc = dc;
        return this;
    }

    public int getChains() {
        return chains;
    }

    public ProxyInfoModel setChains(int chains) {
        this.chains = chains;
        return this;
    }
}
