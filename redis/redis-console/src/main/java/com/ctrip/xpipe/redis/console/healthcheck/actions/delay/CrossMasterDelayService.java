package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.model.DcClusterShard;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Component
public class CrossMasterDelayService implements DelayActionListener, BiDirectionSupport {

    private Map<DcClusterShard, Map<String, Pair<HostPort, Long>>> crossMasterDelays = Maps.newConcurrentMap();

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @Autowired
    private ConsoleServiceManager consoleServiceManager;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Override
    public void onAction(DelayActionContext context) {
        RedisHealthCheckInstance instance = context.instance();
        RedisInstanceInfo info = instance.getRedisInstanceInfo();
        String targetDcId = info.getDcId();
        DcClusterShard key = new DcClusterShard(currentDcId, info.getClusterId(), info.getShardId());

        if (!currentDcId.equalsIgnoreCase(targetDcId)) {
            if (!crossMasterDelays.containsKey(key)) crossMasterDelays.put(key, Maps.newConcurrentMap());
            crossMasterDelays.get(key).put(targetDcId, Pair.of(context.instance().getRedisInstanceInfo().getHostPort(), context.getResult()));
        }
    }

    @Override
    public void stopWatch(HealthCheckAction action) {
        RedisHealthCheckInstance instance = action.getActionInstance();
        RedisInstanceInfo info = instance.getRedisInstanceInfo();
        DcClusterShard key = new DcClusterShard(currentDcId, info.getClusterId(), info.getShardId());

        if (currentDcId.equalsIgnoreCase(info.getDcId())) {
            crossMasterDelays.remove(key);
        } else if (crossMasterDelays.containsKey(key)) {
            crossMasterDelays.get(key).remove(info.getDcId());
        }
    }

    public Map<String, Pair<HostPort, Long>> getPeerMasterDelayFromCurrentDc(String clusterId, String shardId) {
        Map<String, Pair<HostPort, Long>> peerMasterDelays = crossMasterDelays.get(new DcClusterShard(currentDcId, clusterId, shardId));
        if (null == peerMasterDelays) return null;

        Map<String, Pair<HostPort, Long>> result = new HashMap<>(peerMasterDelays.size());
        peerMasterDelays.forEach((targetDc, delay) -> {
            if (delay.getValue() > 0) {
                result.put(targetDc, Pair.of(delay.getKey(), TimeUnit.NANOSECONDS.toMillis(delay.getValue())));
            } else {
                result.put(targetDc, Pair.of(delay.getKey(), delay.getValue()));
            }
        });

        return result;
    }

    public Map<String, Pair<HostPort, Long>> getPeerMasterDelayFromSourceDc(String sourceDcId, String clusterId, String shardId) {
        if (currentDcId.equalsIgnoreCase(sourceDcId)) {
            return getPeerMasterDelayFromCurrentDc(clusterId, shardId);
        } else {
            try {
                return consoleServiceManager.getCrossMasterDelay(sourceDcId, clusterId, shardId);
            } catch (Exception e) {
                return Collections.emptyMap();
            }
        }
    }

    public Map<String, Pair<HostPort, Long>> getPeerMasterDelayFromSourceDc(ClusterType clusterType, String sourceDcId, String clusterId, String shardId) {
        if (consoleConfig.getOwnClusterType().contains(clusterType.toString())) {
            return getPeerMasterDelayFromSourceDc(sourceDcId, clusterId, shardId);
        } else {
            return consoleServiceManager.getCrossMasterDelayFromParallelService(sourceDcId, clusterId, shardId);
        }
    }

    public UnhealthyInfoModel getCurrentDcUnhealthyMasters() {
        UnhealthyInfoModel unhealthyInfo = new UnhealthyInfoModel();
        for (DcClusterShard dcClusterShard : crossMasterDelays.keySet()) {
            for (Pair<HostPort, Long> targetDcDelay : crossMasterDelays.get(dcClusterShard).values()) {
                Long delay = targetDcDelay.getValue();
                if (null == delay || delay < 0 || delay == DelayAction.SAMPLE_LOST_BUT_PONG) {
                    unhealthyInfo.addUnhealthyInstance(dcClusterShard.getClusterId(), dcClusterShard.getDcId(),
                            dcClusterShard.getShardId(), findMasterOf(dcClusterShard));
                    break;
                }
            }
        }

        return unhealthyInfo;
    }

    private HostPort findMasterOf(DcClusterShard dcClusterShard) {
        String dcId = dcClusterShard.getDcId();
        String clusterId = dcClusterShard.getClusterId();
        String shardId = dcClusterShard.getShardId();
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();

        if (!xpipeMeta.getDcs().containsKey(dcId)) return null;
        DcMeta dcMeta = xpipeMeta.getDcs().get(dcId);

        if (!dcMeta.getClusters().containsKey(clusterId)) return null;
        ClusterMeta clusterMeta = dcMeta.getClusters().get(clusterId);

        if (!clusterMeta.getShards().containsKey(shardId)) return null;
        ShardMeta shardMeta = clusterMeta.getShards().get(shardId);

        Optional<RedisMeta> masterOptional = shardMeta.getRedises().stream().filter(RedisMeta::isMaster).findFirst();
        if (!masterOptional.isPresent()) return null;
        RedisMeta masterMeta = masterOptional.get();

        return new HostPort(masterMeta.getIp(), masterMeta.getPort());
    }

}
