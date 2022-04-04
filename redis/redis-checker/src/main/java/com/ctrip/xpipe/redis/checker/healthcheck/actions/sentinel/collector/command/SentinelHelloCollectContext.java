package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.core.protocal.pojo.SentinelMasterInstance;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SentinelHelloCollectContext {
    private  RedisInstanceInfo info;
    private  Set<SentinelHello> hellos ;
    private  String sentinelMonitorName;
    private  Set<HostPort> sentinels ;
    private  List<HostPort> shardInstances;
    private  HostPort metaMaster;
    private  Map<ClusterType, String[]> clusterTypeSentinelConfig;

    private  Map<HostPort, SentinelMasterInstance> sentinelMonitors = new ConcurrentHashMap<>();
    private  Map<HostPort, Throwable> networkErrorSentinels = new ConcurrentHashMap<>();
    private  Set<HostPort> allMasters = new HashSet<>();
    private  HostPort trueMaster = null;
    private  Set<SentinelHello> toDelete = new HashSet<>();
    private  Set<SentinelHello> toAdd = new HashSet<>();


    public SentinelHelloCollectContext() {}

    public SentinelHelloCollectContext(RedisInstanceInfo info, Set<SentinelHello> hellos,
                                       String sentinelMonitorName, Set<HostPort> sentinels,
                                       HostPort metaMaster, List<HostPort> shardInstances,
                                       Map<ClusterType, String[]> clusterTypeSentinelConfig) {
        this.info = info;
        this.hellos = hellos;
        this.sentinelMonitorName = sentinelMonitorName;
        this.sentinels = sentinels;
        this.metaMaster = metaMaster;
        this.shardInstances = shardInstances;
        this.clusterTypeSentinelConfig = clusterTypeSentinelConfig;
    }

    public RedisInstanceInfo getInfo() {
        return info;
    }

    public Set<SentinelHello> getHellos() {
        return hellos;
    }

    public String getSentinelMonitorName() {
        return sentinelMonitorName;
    }

    public Set<HostPort> getSentinels() {
        return sentinels;
    }

    public Map<HostPort, SentinelMasterInstance> getSentinelMonitors() {
        return sentinelMonitors;
    }

    public Map<HostPort, Throwable> getNetworkErrorSentinels() {
        return networkErrorSentinels;
    }

    public Set<HostPort> getAllMasters() {
        return allMasters;
    }

    public HostPort getTrueMaster() {
        return trueMaster;
    }

    public SentinelHelloCollectContext setTrueMaster(HostPort trueMaster) {
        this.trueMaster = trueMaster;return this;
    }

    public Set<SentinelHello> getToDelete() {
        return toDelete;
    }

    public Set<SentinelHello> getToAdd() {
        return toAdd;
    }

    public HostPort getMetaMaster() {
        return metaMaster;
    }

    public SentinelHelloCollectContext setMetaMaster(HostPort metaMaster) {
        this.metaMaster = metaMaster;return this;
    }

    public List<HostPort> getShardInstances() {
        return shardInstances;
    }

    public SentinelHelloCollectContext setShardInstances(List<HostPort> shardInstances) {
        this.shardInstances = shardInstances;return this;
    }

    public Map<ClusterType, String[]> getClusterTypeSentinelConfig() {
        return clusterTypeSentinelConfig;
    }

    public SentinelHelloCollectContext setClusterTypeSentinelConfig(Map<ClusterType, String[]> clusterTypeSentinelConfig) {
        this.clusterTypeSentinelConfig = clusterTypeSentinelConfig;return this;
    }

    public SentinelHelloCollectContext setInfo(RedisInstanceInfo info) {
        this.info = info;return this;
    }

    public SentinelHelloCollectContext setHellos(Set<SentinelHello> hellos) {
        this.hellos = hellos;return this;
    }

    public SentinelHelloCollectContext setSentinelMonitorName(String sentinelMonitorName) {
        this.sentinelMonitorName = sentinelMonitorName;return this;
    }

    public SentinelHelloCollectContext setSentinels(Set<HostPort> sentinels) {
        this.sentinels = sentinels;return this;
    }

    public SentinelHelloCollectContext setSentinelMonitors(Map<HostPort, SentinelMasterInstance> sentinelMonitors) {
        this.sentinelMonitors = sentinelMonitors;return this;
    }

    public SentinelHelloCollectContext setNetworkErrorSentinels(Map<HostPort, Throwable> networkErrorSentinels) {
        this.networkErrorSentinels = networkErrorSentinels;return this;
    }

    public SentinelHelloCollectContext setAllMasters(Set<HostPort> allMasters) {
        this.allMasters = allMasters;return this;
    }

    public SentinelHelloCollectContext setToDelete(Set<SentinelHello> toDelete) {
        this.toDelete = toDelete;return this;
    }

    public SentinelHelloCollectContext setToAdd(Set<SentinelHello> toAdd) {
        this.toAdd = toAdd;
        return this;
    }
}
