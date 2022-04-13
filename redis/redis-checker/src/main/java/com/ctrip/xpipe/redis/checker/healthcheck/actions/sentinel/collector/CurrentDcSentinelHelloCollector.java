package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.exception.MasterNotFoundException;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.utils.IpUtils;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class CurrentDcSentinelHelloCollector extends DefaultSentinelHelloCollector {

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter();

    @Override
    protected String getSentinelMonitorName(RedisInstanceInfo info) {
        return SentinelUtil.getSentinelMonitorName(info.getClusterId(), info.getShardId(), info.getDcId());
    }

    @Override
    protected Set<HostPort> getSentinels(RedisInstanceInfo info) {
        String dcId = info.getDcId();
        String clusterId = info.getClusterId();
        String shardId = info.getShardId();
        if (!checkDcClusterShardExist(dcId, clusterId, shardId)) {
            return Collections.emptySet();
        }

        DcMeta dcMeta = metaCache.getXpipeMeta().getDcs().get(dcId);
        ShardMeta shardMeta = dcMeta.getClusters().get(clusterId).getShards().get(shardId);
        SentinelMeta sentinelMeta = dcMeta.getSentinels().get(shardMeta.getSentinelId());
        if (null == sentinelMeta) return Collections.emptySet();

        return new HashSet<>(IpUtils.parseAsHostPorts(sentinelMeta.getAddress()));
    }

    @Override
    protected HostPort getMaster(RedisInstanceInfo info) throws MasterNotFoundException {
        String dcId = info.getDcId();
        String clusterId = info.getClusterId();
        String shardId = info.getShardId();
        if (!checkDcClusterShardExist(dcId, clusterId, shardId)) {
            throw new MasterNotFoundException(clusterId, shardId);
        }

        ShardMeta shardMeta = metaCache.getXpipeMeta().getDcs().get(dcId).getClusters().get(clusterId).getShards().get(shardId);
        Optional<RedisMeta> optional = shardMeta.getRedises().stream().filter(RedisMeta::isMaster).findFirst();
        if (!optional.isPresent()) throw new MasterNotFoundException(clusterId, shardId);

        RedisMeta master = optional.get();
        return new HostPort(master.getIp(), master.getPort());
    }


    private boolean checkDcClusterShardExist(String dcId, String clusterId, String shardId) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        return null != xpipeMeta
                && xpipeMeta.getDcs().containsKey(dcId)
                && xpipeMeta.getDcs().get(dcId).getClusters().containsKey(clusterId)
                && xpipeMeta.getDcs().get(dcId).getClusters().get(clusterId).getShards().containsKey(shardId);
    }

    @Override
    protected List<HostPort> getShardInstances(RedisInstanceInfo info) {
        return metaCache.getRedisOfDcClusterShard(info.getClusterId(),info.getShardId(),currentDc).stream().map(redisMeta -> new HostPort(redisMeta.getIp(),redisMeta.getPort())).collect(Collectors.toList());
    }
}
