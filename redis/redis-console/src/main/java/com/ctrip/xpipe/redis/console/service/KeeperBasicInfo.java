package com.ctrip.xpipe.redis.console.service;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 17, 2017
 */
public class KeeperBasicInfo {

    private long keeperContainerId;
    private String host;
    private int port;

    public KeeperBasicInfo() {
    }

    public KeeperBasicInfo(long keeperContainerId, String host, int port) {
        this.keeperContainerId = keeperContainerId;
        this.host = host;
        this.port = port;
    }

    public long getKeeperContainerId() {
        return keeperContainerId;
    }

    public void setKeeperContainerId(long keeperContainerId) {
        this.keeperContainerId = keeperContainerId;
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

    @Override
    public String toString() {
        return String.format("[%s:%d, keeperContainerId:%d]", host, port, keeperContainerId);
    }

}
