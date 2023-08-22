package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.info;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class RedisUsedMemoryCollector implements RedisInfoActionListener, KeeperSupport {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected ConcurrentMap<DcClusterShard, Long> dcClusterShardUsedMemory = new ConcurrentHashMap<>();

    @Override
    public void onAction(RedisInfoActionContext context) {
        try {
            InfoResultExtractor extractor = context.getResult();
            RedisInstanceInfo info = context.instance().getCheckInfo();

            long usedMemory = extractor.getSwapUsedDbSize() == null ?  extractor.getUsedMemory() : extractor.getUsedMemory() + extractor.getSwapUsedDbSize();
            dcClusterShardUsedMemory.put(new DcClusterShard(info.getDcId(), info.getClusterId(), info.getShardId()), usedMemory);
        } catch (Throwable throwable) {
            logger.error("get used memory of redis:{} error: ", context.instance().getCheckInfo().getHostPort(), throwable);
        }
    }

    @Override
    public void stopWatch(HealthCheckAction action) {

    }

    public ConcurrentMap<DcClusterShard, Long> getDcClusterShardUsedMemory() {
        return dcClusterShardUsedMemory;
    }
}
