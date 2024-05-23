package com.ctrip.xpipe.redis.console.keeper.entity;

import com.ctrip.xpipe.redis.checker.model.DcClusterShardKeeper;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.*;

import java.util.HashMap;
import java.util.Map;

public class IPPairData {
    private long inputFlow;
    private long peerData;

    private Map<DcClusterShardKeeper, KeeperUsedInfo> entryMap = new HashMap<>();

    public IPPairData() {
    }

    public void removeDcClusterShard(Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> migrateDcClusterShard) {
        this.inputFlow -= migrateDcClusterShard.getValue().getInputFlow();
        this.peerData -= migrateDcClusterShard.getValue().getPeerData();
        this.entryMap.remove(migrateDcClusterShard.getKey());
    }

    public void addDcClusterShard(Map.Entry<DcClusterShardKeeper, KeeperUsedInfo> migrateDcClusterShard) {
        this.inputFlow += migrateDcClusterShard.getValue().getInputFlow();
        this.peerData += migrateDcClusterShard.getValue().getPeerData();
        this.entryMap.put(migrateDcClusterShard.getKey(), migrateDcClusterShard.getValue());
    }

    public long getInputFlow() {
        return inputFlow;
    }

    public long getPeerData() {
        return peerData;
    }

    public Map<DcClusterShardKeeper, KeeperUsedInfo> getEntryMap() {
        return entryMap;
    }
}
