package com.ctrip.xpipe.redis.console.healthcheck.action;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */

public class DefaultPingDownStrategy implements PingDownStrategy {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Override
    public PingDownResult getPingDownResult(Collection<RedisHealthCheckInstance> pingDownInstances) {
        Map<Pair<String, String>, List<RedisHealthCheckInstance>> map = Maps.newHashMap();
        for(RedisHealthCheckInstance instance : pingDownInstances) {
            RedisInstanceInfo info = instance.getRedisInstanceInfo();
            Pair<String, String> key = new Pair<>(info.getDcId(), info.getClusterId());
            List<RedisHealthCheckInstance> list = map.getOrDefault(key,
                    Lists.newArrayListWithExpectedSize(pingDownInstances.size()));
            list.add(instance);
            map.put(key, list);
        }

        DefaultPingDownResult result = new DefaultPingDownResult();

        for(Map.Entry<Pair<String, String>, List<RedisHealthCheckInstance>> entry : map.entrySet()) {
            Pair<String, String> dcCluster = entry.getKey();
            List<RedisHealthCheckInstance> instances = entry.getValue();
            float numbers = metaCache.getRedisNumOfDcCluster(dcCluster.getKey(), dcCluster.getValue());
            float pingDownFactor = consoleConfig.getPingDownMajorityRatio();
            if(((float)instances.size()) >= numbers * pingDownFactor) {
                result.addIgnoredInstances(instances);
            } else {
                result.addPingDownInstances(instances);
            }
        }
        return result;
    }



    class DefaultPingDownResult implements PingDownResult {

        private List<RedisHealthCheckInstance> pingDownInstances = Lists.newArrayList();

        private List<RedisHealthCheckInstance> ignoredInstances = Lists.newArrayList();

        @Override
        public List<RedisHealthCheckInstance> getPingDownInstances() {
            return pingDownInstances;
        }

        @Override
        public List<RedisHealthCheckInstance> getIgnoredPingDownInstances() {
            return ignoredInstances;
        }

        public void addPingDownInstances(List<RedisHealthCheckInstance> instances) {
            pingDownInstances.addAll(instances);
        }

        public void addIgnoredInstances(List<RedisHealthCheckInstance> instances) {
            ignoredInstances.addAll(instances);
        }
    }
}
