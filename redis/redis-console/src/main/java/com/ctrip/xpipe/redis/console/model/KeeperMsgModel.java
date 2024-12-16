package com.ctrip.xpipe.redis.console.model;

public class KeeperMsgModel {

    private String ip;
    private int port;
    private String clusterName;
    private String shardName;
    private boolean active;
    private String role;
    private String err;

    public KeeperMsgModel(String ip, int port, String clusterName, String shardName) {
        this.ip = ip;
        this.port = port;
        this.clusterName = clusterName;
        this.shardName = shardName;
    }

    public KeeperMsgModel(String clusterName, String shardName) {
        this.clusterName = clusterName;
        this.shardName = shardName;
    }

    public KeeperMsgModel(String err) {
        this.err = err;
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

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getShardName() {
        return shardName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getErr() {
        return err;
    }

    public void setErr(String err) {
        this.err = err;
    }

    public void addErr(String err) {
        this.err += err;
    }

    @Override
    public String toString() {
        return "KeeperMsgModel{" +
                "ip='" + ip + '\'' +
                ", port=" + port +
                ", clusterName='" + clusterName + '\'' +
                ", shardName='" + shardName + '\'' +
                ", active=" + active +
                ", role='" + role + '\'' +
                ", err='" + err + '\'' +
                '}';
    }
}
