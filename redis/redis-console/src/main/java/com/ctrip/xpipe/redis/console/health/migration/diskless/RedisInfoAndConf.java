package com.ctrip.xpipe.redis.console.health.migration.diskless;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2017
 */
public class RedisInfoAndConf {

    private String serverInfo;

    private List<String> serverConf;

    public String getServerInfo() {
        return serverInfo;
    }

    public void setServerInfo(String serverInfo) {
        this.serverInfo = serverInfo;
    }

    public List<String> getServerConf() {
        return serverConf;
    }

    public void setServerConf(List<String> serverConf) {
        this.serverConf = serverConf;
    }
}
