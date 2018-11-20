package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

public class ProxyChainModel {

    private String shardId;

    private String redisMaster;

    private String activeDcKeeper;

    private String backupDcKeeper;

    private List<TunnelInfo> tunnels;

    private final static String UNKNOWN = "UNKNOWN:-1";

    public ProxyChainModel(ProxyChain chain, RedisTbl master, RedisTbl acitveDcKeeperTbl, RedisTbl backupDcKeeperTbl) {
        this.shardId = chain.getShard();
        this.redisMaster = master == null ?  UNKNOWN : new HostPort(master.getRedisIp(), master.getRedisPort()).toString();
        this.activeDcKeeper = acitveDcKeeperTbl == null ?  UNKNOWN :new HostPort(acitveDcKeeperTbl.getRedisIp(), acitveDcKeeperTbl.getRedisPort()).toString();
        this.backupDcKeeper = backupDcKeeperTbl == null ?  UNKNOWN :new HostPort(backupDcKeeperTbl.getRedisIp(), backupDcKeeperTbl.getRedisPort()).toString();
        this.tunnels = chain.getTunnels();
    }

    public String getShardId() {
        return shardId;
    }

    public String getRedisMaster() {
        return redisMaster;
    }

    public String getActiveDcKeeper() {
        return activeDcKeeper;
    }

    public String getBackupDcKeeper() {
        return backupDcKeeper;
    }

    public List<TunnelInfo> getTunnels() {
        return tunnels;
    }
}
