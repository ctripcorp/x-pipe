package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.controller;

import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.CurrentDcCheckController;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionController;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CurrentDcSentinelCheckController extends CurrentDcCheckController implements SentinelActionController, BiDirectionSupport, SingleDcSupport, LocalDcSupport {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    public CurrentDcSentinelCheckController(FoundationService foundationService) {
        super(foundationService.getDataCenter());
    }

    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getCheckInfo();
        return super.shouldCheck(instance)
                && !(getRedisCntInCurrentDc(info.getClusterId(), info.getShardId()) > 1 && info.isMaster());
    }

    private int getRedisCntInCurrentDc(String clusterId, String shardId) {
        return metaCache.getRedisOfDcClusterShard(currentDcId, clusterId, shardId).size();
    }

}
