package com.ctrip.xpipe.redis.console.healthcheck.fulllink.model;

import java.util.Map;

public class KeeperSlaveModel {

    private String host;
    private int port;
    private String state;
    private long offset;
    private long remotePort;
    public static final String SLAVE_IP = "ip";
    public static final String SLAVE_PORT = "port";
    public static final String SLAVE_STATE = "state";
    public static final String SLAVE_OFFSET = "offset";
    public static final String SLAVE_REMOTE_PORT = "remotePort";

    public KeeperSlaveModel() {
    }

    public KeeperSlaveModel(String host, int port, String state, long offset, long remotePort) {
        this.host = host;
        this.port = port;
        this.state = state;
        this.offset = offset;
        this.remotePort = remotePort;
    }

    public KeeperSlaveModel(Map<String, String> stringMap) {
        this.host = stringMap.get(SLAVE_IP);
        this.port = Integer.parseInt(stringMap.get(SLAVE_PORT));
        this.state = stringMap.get(SLAVE_STATE);
        this.offset = Long.parseLong(stringMap.get(SLAVE_OFFSET));
        this.remotePort = Long.parseLong(stringMap.get(SLAVE_REMOTE_PORT));
    }

    public String getHost() {
        return host;
    }

    public KeeperSlaveModel setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public KeeperSlaveModel setPort(int port) {
        this.port = port;
        return this;
    }

    public String getState() {
        return state;
    }

    public KeeperSlaveModel setState(String state) {
        this.state = state;
        return this;
    }

    public long getOffset() {
        return offset;
    }

    public KeeperSlaveModel setOffset(long offset) {
        this.offset = offset;
        return this;
    }

    public long getRemotePort() {
        return remotePort;
    }

    public KeeperSlaveModel setRemotePort(long remotePort) {
        this.remotePort = remotePort;
        return this;
    }
}


