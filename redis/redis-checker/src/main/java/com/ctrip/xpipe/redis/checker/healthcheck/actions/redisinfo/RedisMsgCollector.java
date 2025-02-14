package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.RedisMsg;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.ctrip.xpipe.cluster.ClusterType.ONE_WAY;

@Component
public class RedisMsgCollector implements InfoActionListener, OneWaySupport {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected Map<String, Map<HostPort, RedisMsg>> redisMsgMap = new ConcurrentHashMap<>();

    public static final String ROR_DB_VERSION = "1.3";

    @Override
    public void onAction(RawInfoActionContext context) {
        try {
            InfoResultExtractor extractor = new InfoResultExtractor(context.getResult());
            RedisInstanceInfo info = context.instance().getCheckInfo();
            if (info.getClusterType() != ONE_WAY) return;
            long usedMemory = getUsedMemory(extractor);
            if (!redisMsgMap.containsKey(info.getDcId())) {
                redisMsgMap.put(info.getDcId(), new ConcurrentHashMap<>());
            }
            RedisMsg redisMsg = redisMsgMap.get(info.getDcId()).get(info.getHostPort());
            if (redisMsg == null) {
                redisMsg = new RedisMsg(0, usedMemory, extractor.getMasterReplOffset());
            }
            int checkIntervalSec = context.instance().getHealthCheckConfig().checkIntervalMilli()/ 1000;
            long inPutFlow = (extractor.getMasterReplOffset() - redisMsg.getOffset())/1024/1024/checkIntervalSec;
            redisMsg = new RedisMsg(inPutFlow, usedMemory, extractor.getMasterReplOffset());
            redisMsgMap.get(info.getDcId()).put(info.getHostPort(), redisMsg);
        } catch (Throwable throwable) {
            logger.error("get msg of redis:{} error: ", context.instance().getCheckInfo().getHostPort(), throwable);
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
        Map<HostPort, RedisMsg> hostPortRedisMsgMap = redisMsgMap.get(info.getDcId());
        if (hostPortRedisMsgMap == null) {
            return;
        }
        hostPortRedisMsgMap.remove(info.getHostPort());
    }

    public Map<String, Map<HostPort, RedisMsg>> getRedisMsgMap() {
        return redisMsgMap;
    }

}
