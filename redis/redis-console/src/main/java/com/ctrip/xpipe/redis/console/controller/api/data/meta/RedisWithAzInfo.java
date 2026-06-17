package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * @author yihaohuang
 * <p>
 * Jun 8, 2026
 */
public class RedisWithAzInfo extends AbstractCreateInfo {
    private String addr;
    private String azName;

    public RedisWithAzInfo() {
    }

    @JsonCreator
    public RedisWithAzInfo(String addr) {
        this.addr = addr;
    }

    @Override
    public void check() throws CheckFailException {
    }

    public String getAddr() {
        return addr;
    }

    public RedisWithAzInfo setAddr(String addr) {
        this.addr = addr;
        return this;
    }

    public String getAzName() {
        return azName;
    }

    public RedisWithAzInfo setAzName(String azName) {
        this.azName = azName;
        return this;
    }
}
