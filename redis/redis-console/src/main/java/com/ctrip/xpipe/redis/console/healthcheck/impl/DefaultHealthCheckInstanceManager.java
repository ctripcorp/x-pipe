package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
@Component
public class DefaultHealthCheckInstanceManager implements HealthCheckInstanceManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultHealthCheckInstanceManager.class);

    private ConcurrentMap<HostPort, RedisHealthCheckInstance> instances = Maps.newConcurrentMap();

    @Autowired
    private RedisHealthCheckInstanceFactory instanceFactory;

    @Override
    public RedisHealthCheckInstance getOrCreate(RedisMeta redis) {
        HostPort key = new HostPort(redis.getIp(), redis.getPort());
        return MapUtils.getOrCreate(instances, key, new ObjectFactory<RedisHealthCheckInstance>() {
            @Override
            public RedisHealthCheckInstance create() {
                return instanceFactory.create(redis);
            }
        });
    }

    @Override
    public RedisHealthCheckInstance findRedisHealthCheckInstance(HostPort hostPort) {
        return instances.get(hostPort);
    }

    @Override
    public void remove(HostPort hostPort) {
        RedisHealthCheckInstance instance = instances.remove(hostPort);
        try {
            LifecycleHelper.stopIfPossible(instance);
        } catch (Exception e) {
            logger.error("[remove]", e);
        }
    }

    @Override
    public List<RedisHealthCheckInstance> getAllRedisInstance() {
        return Lists.newLinkedList(instances.values());
    }

}
