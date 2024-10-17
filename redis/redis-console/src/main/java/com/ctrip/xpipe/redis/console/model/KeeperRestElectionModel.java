package com.ctrip.xpipe.redis.console.model;


public class KeeperRestElectionModel {

    private String ip;
    private String port;
    private String shardId;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    @Override
    public String toString() {
        return "KeeperRestElectionModel{" +
                "ip='" + ip + '\'' +
                ", port=" + port +
                ", shardId=" + shardId +
                '}';
    }
}
