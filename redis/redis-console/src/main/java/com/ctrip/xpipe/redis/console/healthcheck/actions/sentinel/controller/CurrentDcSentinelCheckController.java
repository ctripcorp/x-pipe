package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.controller;

import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.CurrentDcCheckController;
import com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.SentinelActionController;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CurrentDcSentinelCheckController extends CurrentDcCheckController implements SentinelActionController, BiDirectionSupport {

    @Autowired
    private MetaCache metaCache;

    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getRedisInstanceInfo();
        return super.shouldCheck(instance)
                && !(getRedisCntInCurrentDc(info.getClusterId(), info.getShardId()) > 1 && info.isMaster());
    }

    private int getRedisCntInCurrentDc(String clusterId, String shardId) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return 0;

        DcMeta dcMeta = xpipeMeta.getDcs().get(currentDcId);
        if (null == dcMeta) return 0;

        ClusterMeta clusterMeta = dcMeta.getClusters().get(clusterId);
        if (null == clusterMeta) return 0;

        ShardMeta shardMeta = clusterMeta.getShards().get(shardId);
        if (null == shardMeta) return 0;

        return null == shardMeta.getRedises() ? 0 : shardMeta.getRedises().size();
    }

}
