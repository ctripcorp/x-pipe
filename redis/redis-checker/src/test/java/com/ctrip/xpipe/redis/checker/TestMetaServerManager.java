package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/3/15
 */
public class TestMetaServerManager implements MetaServerManager {

    private Map<DcClusterShard, RedisMeta> masters = new HashMap<>();

    public void setMaster(String dcId, String clusterId, String shardId, RedisMeta master) {
        masters.put(new DcClusterShard(dcId, clusterId, shardId), master);
    }

    @Override
    public RedisMeta getCurrentMaster(String dcId, String clusterId, String shardId) {
        return masters.get(new DcClusterShard(dcId, clusterId, shardId));
    }
}
