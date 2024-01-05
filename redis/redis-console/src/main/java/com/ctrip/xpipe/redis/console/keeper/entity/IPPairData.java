package com.ctrip.xpipe.redis.console.keeper.entity;

import com.ctrip.xpipe.redis.checker.model.DcClusterShardActive;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.*;

import java.util.HashMap;
import java.util.Map;

public class IPPairData {
    private long inputFlow;
    private long peerData;
    private final Map<DcClusterShardActive, KeeperUsedInfo> keeperUsedInfoMap = new HashMap<>();

    public IPPairData() {
    }

    public void removeDcClusterShard(Map.Entry<DcClusterShardActive, KeeperUsedInfo> migrateDcClusterShard) {
        this.inputFlow -= migrateDcClusterShard.getValue().getInputFlow();
        this.peerData -= migrateDcClusterShard.getValue().getPeerData();
        keeperUsedInfoMap.remove(migrateDcClusterShard.getKey());
    }

    public void addDcClusterShard(Map.Entry<DcClusterShardActive, KeeperUsedInfo> migrateDcClusterShard) {
        this.inputFlow += migrateDcClusterShard.getValue().getInputFlow();
        this.peerData += migrateDcClusterShard.getValue().getPeerData();
        keeperUsedInfoMap.put(migrateDcClusterShard.getKey(), migrateDcClusterShard.getValue());
    }

    public long getInputFlow() {
        return inputFlow;
    }

    public long getPeerData() {
        return peerData;
    }

    public Map<DcClusterShardActive, KeeperUsedInfo> getKeeperUsedInfoMap() {
        return keeperUsedInfoMap;
    }

}
