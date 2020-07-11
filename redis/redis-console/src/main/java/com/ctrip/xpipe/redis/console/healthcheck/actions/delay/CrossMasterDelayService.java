package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.model.DcClusterShard;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;

@Component
public class CrossMasterDelayService implements DelayActionListener, BiDirectionSupport {

    private Map<DcClusterShard, Map<String, Pair<HostPort, Long>>> crossMasterDelays = Maps.newConcurrentMap();

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @Autowired
    private ConsoleServiceManager consoleServiceManager;

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
        return crossMasterDelays.get(new DcClusterShard(currentDcId, clusterId, shardId));
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

}
