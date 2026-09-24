package com.ctrip.xpipe.redis.console.model;


public class KeeperRestElectionModel {

    private String ip;
    private String port;
    private String shardId;
    private String hostname;
    /** keeper 所在机房，用于把 hostname→IP 反查直接定向到该机房的 console，避免广播查询所有机房 */
    private String dcName;

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

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getDcName() {
        return dcName;
    }

    public void setDcName(String dcName) {
        this.dcName = dcName;
    }

    @Override
    public String toString() {
        return "KeeperRestElectionModel{" +
                "ip='" + ip + '\'' +
                ", hostname='" + hostname + '\'' +
                ", dcName='" + dcName + '\'' +
                ", port=" + port +
                ", shardId=" + shardId +
                '}';
    }
}
