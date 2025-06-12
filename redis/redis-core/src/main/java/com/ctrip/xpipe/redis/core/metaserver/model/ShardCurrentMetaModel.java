package com.ctrip.xpipe.redis.core.metaserver.model;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;

public class ShardCurrentMetaModel {

    private long shardDbId;
    private List<KeeperMeta> surviveKeepers;
    private Pair<String, Integer> keeperMaster;

    private ShardMeta shardMeta;

    public ShardMeta getShardMeta() {
        return shardMeta;
    }

    public ShardCurrentMetaModel setShardMeta(ShardMeta shardMeta) {
        this.shardMeta = shardMeta;
        return this;
    }

    public long getShardDbId() {
        return shardDbId;
    }

    public ShardCurrentMetaModel setShardDbId(long shardDbId) {
        this.shardDbId = shardDbId;
        return this;
    }

    public List<KeeperMeta> getSurviveKeepers() {
        return surviveKeepers;
    }

    public ShardCurrentMetaModel setSurviveKeepers(List<KeeperMeta> surviveKeeperModels) {
        this.surviveKeepers = surviveKeeperModels;
        return this;
    }

    public Pair<String, Integer> getKeeperMaster() {
        return keeperMaster;
    }

    public ShardCurrentMetaModel setKeeperMaster(Pair<String, Integer> keeperMaster) {
        this.keeperMaster = keeperMaster;
        return this;
    }
}
