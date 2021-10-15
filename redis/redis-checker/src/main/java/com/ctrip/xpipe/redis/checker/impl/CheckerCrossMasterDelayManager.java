package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.CrossMasterDelayManager;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionListener;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public class CheckerCrossMasterDelayManager implements CrossMasterDelayManager, DelayActionListener, BiDirectionSupport {

//    protected static final String CURRENT_DC = FoundationService.DEFAULT.getDataCenter();

    protected Map<DcClusterShard, Map<String, Pair<HostPort, Long>>> crossMasterDelays = Maps.newConcurrentMap();

    protected String currentDcId;

    public CheckerCrossMasterDelayManager(String currentDcId) {
        this.currentDcId = currentDcId;
    }
    
    @Override
    public void onAction(DelayActionContext context) {
        RedisHealthCheckInstance instance = context.instance();
        RedisInstanceInfo info = instance.getCheckInfo();
        String targetDcId = info.getDcId();
        DcClusterShard key = new DcClusterShard(currentDcId, info.getClusterId(), info.getShardId());

        if (!currentDcId.equalsIgnoreCase(targetDcId)) {
            if (!crossMasterDelays.containsKey(key)) crossMasterDelays.put(key, Maps.newConcurrentMap());
            crossMasterDelays.get(key).put(targetDcId, Pair.of(context.instance().getCheckInfo().getHostPort(), context.getResult()));
        }
    }

    @Override
    public void stopWatch(HealthCheckAction<RedisHealthCheckInstance> action) {
        RedisHealthCheckInstance instance = action.getActionInstance();
        RedisInstanceInfo info = instance.getCheckInfo();
        DcClusterShard key = new DcClusterShard(currentDcId, info.getClusterId(), info.getShardId());

        if (currentDcId.equalsIgnoreCase(info.getDcId())) {
            crossMasterDelays.remove(key);
        } else if (crossMasterDelays.containsKey(key)) {
            crossMasterDelays.get(key).remove(info.getDcId());
        }
    }

    @Override
    public Map<DcClusterShard, Map<String, Pair<HostPort, Long>>> getAllCrossMasterDelays() {
        return new HashMap<>(crossMasterDelays);
    }

    @Override
    public void updateCrossMasterDelays(Map<DcClusterShard, Map<String, Pair<HostPort, Long>>> delays) {
        throw new UnsupportedOperationException("updateCrossMasterDelays not upport");
    }
}
