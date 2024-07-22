package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.model.RedisRoleModel;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisConsoleSessionManager;
import com.ctrip.xpipe.redis.console.service.RedisSessionService;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.pojo.AbstractRole;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;


@Component
public class RedisSessionServiceImpl implements RedisSessionService {

    @Autowired
    private RedisConsoleSessionManager sessionManager;

    @Autowired
    private MetaCache metaCache;

    @Override
    public CommandFuture<Role> getRedisRole(String ip, int port, RedisSession.RollCallback callback) {
        return sessionManager.findOrCreateSession(new DefaultEndPoint(ip, port)).role(callback);
    }

    @Override
    public List<RedisRoleModel> getShardAllRedisRole(String dcId, String clusterId, String shardId) {
        List<RedisMeta> redisOfDcClusterShard = metaCache.getRedisOfDcClusterShard(dcId, clusterId, shardId);
        List<RedisRoleModel> models = new CopyOnWriteArrayList<>();
        Map<HostPort, CommandFuture<Role>> futureMap = new HashMap<>();
        redisOfDcClusterShard.forEach(redisMeta -> {
            CommandFuture<Role> redisRole = getRedisRole(redisMeta.getIp(), redisMeta.getPort(), new RedisSession.RollCallback() {
                @Override
                public void role(String role, Role detail) {
                    models.add(new RedisRoleModel(redisMeta.getIp(), redisMeta.getPort(), (AbstractRole) detail));
                }

                @Override
                public void fail(Throwable e) {
                    models.add(new RedisRoleModel(redisMeta.getIp(), redisMeta.getPort(),e));
                }
            });
            futureMap.put(new HostPort(redisMeta.getIp(), redisMeta.getPort()), redisRole);
        });

        for (Map.Entry<HostPort, CommandFuture<Role>> entry : futureMap.entrySet()) {
            try {
                entry.getValue().get();
            } catch (InterruptedException | ExecutionException e) {
                models.add(new RedisRoleModel(entry.getKey().getHost(), entry.getKey().getPort(), e));
            }
        }
        return models;
    }

}
