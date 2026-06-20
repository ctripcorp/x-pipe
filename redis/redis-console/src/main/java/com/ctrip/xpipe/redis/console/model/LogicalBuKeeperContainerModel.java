package com.ctrip.xpipe.redis.console.model;

import java.io.Serializable;

public class LogicalBuKeeperContainerModel implements Serializable {

    private static final long serialVersionUID = 1L;

    private long id;
    private String ip;
    private int port;
    private boolean active;
    private String diskType;
    private String tag;

    public long getId() {
        return id;
    }

    public LogicalBuKeeperContainerModel setId(long id) {
        this.id = id;
        return this;
    }

    public String getIp() {
        return ip;
    }

    public LogicalBuKeeperContainerModel setIp(String ip) {
        this.ip = ip;
        return this;
    }

    public int getPort() {
        return port;
    }

    public LogicalBuKeeperContainerModel setPort(int port) {
        this.port = port;
        return this;
    }

    public boolean isActive() {
        return active;
    }

    public LogicalBuKeeperContainerModel setActive(boolean active) {
        this.active = active;
        return this;
    }

    public String getDiskType() {
        return diskType;
    }

    public LogicalBuKeeperContainerModel setDiskType(String diskType) {
        this.diskType = diskType;
        return this;
    }

    public String getTag() {
        return tag;
    }

    public LogicalBuKeeperContainerModel setTag(String tag) {
        this.tag = tag;
        return this;
    }
}
