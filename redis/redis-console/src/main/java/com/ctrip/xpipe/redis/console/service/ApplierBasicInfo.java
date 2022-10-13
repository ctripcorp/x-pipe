package com.ctrip.xpipe.redis.console.service;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 17, 2017
 */
public class ApplierBasicInfo {

    private long appliercontainerId;
    private String host;
    private int port;

    public ApplierBasicInfo() {
    }

    public ApplierBasicInfo(long appliercontainerId, String host, int port) {
        this.appliercontainerId = appliercontainerId;
        this.host = host;
        this.port = port;
    }

    public long getAppliercontainerId() {
        return appliercontainerId;
    }

    public ApplierBasicInfo setAppliercontainerId(long appliercontainerId) {
        this.appliercontainerId = appliercontainerId;
        return this;
    }

    public String getHost() {
        return host;
    }

    public ApplierBasicInfo setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public ApplierBasicInfo setPort(int port) {
        this.port = port;
        return this;
    }

    @Override
    public String toString() {
        return "ApplierBasicInfo{" +
                "appliercontainerId=" + appliercontainerId +
                ", host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
