package com.ctrip.xpipe.redis.console.healthcheck.fulllink.model;

import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KeeperStateModel {
    private String host;
    private int port;
    private MASTER_STATE role;
    private String state;
    private String masterHost;
    private int masterPort;
    private long masterReplOffset;
    private long replBacklogSize;
    private List<KeeperSlaveModel> slaves;
    private Long replId;
    private Map<String, Throwable> errs = new ConcurrentHashMap<>();

    public KeeperStateModel() {
    }

    public String getHost() {
        return host;
    }

    public KeeperStateModel setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public KeeperStateModel setPort(int port) {
        this.port = port;
        return this;
    }

    public MASTER_STATE getRole() {
        return role;
    }

    public KeeperStateModel setRole(MASTER_STATE role) {
        this.role = role;
        return this;
    }

    public String getState() {
        return state;
    }

    public KeeperStateModel setState(String state) {
        this.state = state;
        return this;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public KeeperStateModel setMasterHost(String masterHost) {
        this.masterHost = masterHost;
        return this;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public KeeperStateModel setMasterPort(int masterPort) {
        this.masterPort = masterPort;
        return this;
    }

    public long getMasterReplOffset() {
        return masterReplOffset;
    }

    public KeeperStateModel setMasterReplOffset(long masterReplOffset) {
        this.masterReplOffset = masterReplOffset;
        return this;
    }

    public long getReplBacklogSize() {
        return replBacklogSize;
    }

    public KeeperStateModel setReplBacklogSize(long replBacklogSize) {
        this.replBacklogSize = replBacklogSize;
        return this;
    }

    public List<KeeperSlaveModel> getSlaves() {
        return slaves;
    }

    public void setSlaves(List<KeeperSlaveModel> slaves) {
        this.slaves = slaves;
    }

    public KeeperStateModel setSlavesMap(List<Map<String, String>> slaves) {
        this.slaves = new ArrayList<>();
        for (Map<String, String> slave : slaves) {
            this.slaves.add(new KeeperSlaveModel(slave));
        }
        return this;
    }

    public Long getReplId() {
        return replId;
    }

    public KeeperStateModel setReplId(Long replId) {
        this.replId = replId;
        return this;
    }

    public Map<String, Throwable> getErrs() {
        return errs;
    }

    public void setErrs(Map<String, Throwable> errs) {
        this.errs = errs;
    }

    public synchronized void addErr(String msg, Throwable err) {
        errs.put(msg, err);
    }

}
