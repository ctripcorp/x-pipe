package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.core.protocal.pojo.SentinelMasterInstance;
import com.ctrip.xpipe.tuple.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SentinelHelloCollectContext {
    private RedisInstanceInfo info;
    private Set<SentinelHello> collectedHellos;
    private Set<SentinelHello> processedHellos;
    private String sentinelMonitorName;
    private Set<HostPort> sentinels;
    private List<HostPort> shardInstances;
    private HostPort metaMaster;
    private Map<ClusterType, String[]> clusterTypeSentinelConfig;

    private Map<HostPort, SentinelMasterInstance> sentinelMonitors = new ConcurrentHashMap<>();
    private Map<HostPort, Throwable> networkErrorSentinels = new ConcurrentHashMap<>();
    private Set<HostPort> allMasters = new HashSet<>();
    private Pair<HostPort, List<HostPort>> trueMasterInfo;
    private Set<SentinelHello> toDelete = new HashSet<>();
    private Set<SentinelHello> toAdd = new HashSet<>();
    private Set<SentinelHello> toCheckReset = new HashSet<>();


    public SentinelHelloCollectContext() {}

    public SentinelHelloCollectContext(RedisInstanceInfo info, Set<SentinelHello> hellos,
                                       String sentinelMonitorName, Set<HostPort> sentinels,
                                       HostPort metaMaster, List<HostPort> shardInstances,
                                       Map<ClusterType, String[]> clusterTypeSentinelConfig) {
        this.info = info;
        this.collectedHellos = hellos;
        this.processedHellos = new HashSet<>(hellos);
        this.sentinelMonitorName = sentinelMonitorName;
        this.sentinels = sentinels;
        this.metaMaster = metaMaster;
        this.shardInstances = shardInstances;
        this.clusterTypeSentinelConfig = clusterTypeSentinelConfig;
    }

    public RedisInstanceInfo getInfo() {
        return info;
    }

    public Set<SentinelHello> getCollectedHellos() {
        return new HashSet<>(collectedHellos);
    }

    public SentinelHelloCollectContext setCollectedHellos(Set<SentinelHello> collectedHellos) {
        this.collectedHellos = collectedHellos;
        return this;
    }

    public Set<SentinelHello> getProcessedHellos() {
        return processedHellos;
    }

    public SentinelHelloCollectContext setProcessedHellos(Set<SentinelHello> processedHellos) {
        this.processedHellos = processedHellos;
        return this;
    }

    public String getSentinelMonitorName() {
        return sentinelMonitorName;
    }

    public Set<HostPort> getSentinels() {
        return new HashSet<>(sentinels);
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

    public Pair<HostPort, List<HostPort>> getTrueMasterInfo() {
        return trueMasterInfo;
    }

    public SentinelHelloCollectContext setTrueMasterInfo(Pair<HostPort, List<HostPort>> trueMasterInfo) {
        this.trueMasterInfo = trueMasterInfo;
        return this;
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
        return new ArrayList<>(shardInstances);
    }

    public SentinelHelloCollectContext setShardInstances(List<HostPort> shardInstances) {
        this.shardInstances = shardInstances;
        return this;
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

    public Set<SentinelHello> getToCheckReset() {
        return toCheckReset;
    }

    public SentinelHelloCollectContext setToCheckReset(Set<SentinelHello> toCheckReset) {
        this.toCheckReset = toCheckReset;
        return this;
    }
}
