package com.ctrip.xpipe.redis.console.controller.api.data.meta;

/**
 * @author yihaohuang
 * <p>
 * Jun 8, 2026
 */
public class RedisAzUpdateInfo extends AbstractCreateInfo {
    private String dcId;
    private String ip;
    private int port;
    private String azName;

    @Override
    public void check() throws CheckFailException {
    }

    public String getDcId() {
        return dcId;
    }

    public RedisAzUpdateInfo setDcId(String dcId) {
        this.dcId = dcId;
        return this;
    }

    public String getIp() {
        return ip;
    }

    public RedisAzUpdateInfo setIp(String ip) {
        this.ip = ip;
        return this;
    }

    public int getPort() {
        return port;
    }

    public RedisAzUpdateInfo setPort(int port) {
        this.port = port;
        return this;
    }

    public String getAzName() {
        return azName;
    }

    public RedisAzUpdateInfo setAzName(String azName) {
        this.azName = azName;
        return this;
    }
}
