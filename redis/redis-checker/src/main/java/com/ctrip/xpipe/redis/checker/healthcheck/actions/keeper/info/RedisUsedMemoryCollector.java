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
        Long maxMemory = extractor.getMaxMemory();
        Long usedMemory = extractor.getUsedMemory();
        Long dbSize = extractor.getSwapUsedDbSize();

        if (dbSize == null || usedMemory < maxMemory) return usedMemory;

        String keysSpaceDb0 = extractor.extract("db0");
        if (StringUtil.isEmpty(keysSpaceDb0)) return 0;

        String[] keySpaces = keysSpaceDb0.split(",");
        String[] keys1 = keySpaces[0].split("=");
        String[] keys2 = keySpaces[1].split("=");
        if (!keys1[0].equalsIgnoreCase("keys") || !keys2[0].equalsIgnoreCase("evicts")) {
            return usedMemory + dbSize * 3;
        }

        long evicts = Long.parseLong(keys2[1]);
        long keys = Long.parseLong(keys1[1]);
        return keys == 0 ? dbSize * 3 : (keys + evicts) / keys * maxMemory;
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
