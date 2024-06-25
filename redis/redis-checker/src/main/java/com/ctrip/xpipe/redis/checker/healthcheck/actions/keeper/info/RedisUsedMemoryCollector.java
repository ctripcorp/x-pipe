package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.info;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class RedisUsedMemoryCollector implements RedisInfoActionListener, KeeperSupport {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected ConcurrentMap<DcClusterShard, Long> dcClusterShardUsedMemory = new ConcurrentHashMap<>();

    public static final String ROR_DB_VERSION = "1.3";

    @Override
    public void onAction(RedisInfoActionContext context) {
        try {
            InfoResultExtractor extractor = context.getResult();
            RedisInstanceInfo info = context.instance().getCheckInfo();

            long usedMemory = getUsedMemory(extractor);
            dcClusterShardUsedMemory.put(new DcClusterShard(info.getDcId(), info.getClusterId(), info.getShardId()), usedMemory);
        } catch (Throwable throwable) {
            logger.error("get used memory of redis:{} error: ", context.instance().getCheckInfo().getHostPort(), throwable);
        }
    }

    private long getUsedMemory(InfoResultExtractor extractor) {
        String swapVersion = extractor.getKeySwapVersion();
        Long dbSize = extractor.getSwapUsedDbSize();
        Long maxMemory = extractor.getMaxMemory();
        Long usedMemory = extractor.getUsedMemory();
        if (dbSize == null) return usedMemory;
        if (usedMemory < maxMemory) return usedMemory + dbSize;
        if (!StringUtil.isEmpty(swapVersion) && StringUtil.compareVersionSize(swapVersion, ROR_DB_VERSION) >= 0) {
            return dbSize + maxMemory;
        }
        return usedMemory + dbSize * 3;
    }

    @Override
    public void stopWatch(HealthCheckAction action) {
        DefaultRedisInstanceInfo info = (DefaultRedisInstanceInfo) action.getActionInstance().getCheckInfo();
        logger.debug("[stopWatch] DcClusterShard: {}", new DcClusterShard(info.getDcId(), info.getClusterId(), info.getShardId()));
        dcClusterShardUsedMemory.remove(new DcClusterShard(info.getDcId(), info.getClusterId(), info.getShardId()));
    }

    public ConcurrentMap<DcClusterShard, Long> getDcClusterShardUsedMemory() {
        return dcClusterShardUsedMemory;
    }

}
