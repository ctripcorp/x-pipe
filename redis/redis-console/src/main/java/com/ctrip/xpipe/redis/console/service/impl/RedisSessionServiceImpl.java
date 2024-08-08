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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;


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
        Map<HostPort, RedisRoleModel> models = new ConcurrentHashMap<>();
        Map<HostPort, CommandFuture<Role>> futureMap = new HashMap<>();
        redisOfDcClusterShard.forEach(redisMeta -> {
            HostPort redis = new HostPort(redisMeta.getIp(), redisMeta.getPort());
            CommandFuture<Role> redisRole = getRedisRole(redisMeta.getIp(), redisMeta.getPort(), new RedisSession.RollCallback() {
                @Override
                public void role(String role, Role detail) {
                    models.put(redis, new RedisRoleModel(redisMeta.getIp(), redisMeta.getPort(), (AbstractRole) detail));
                }

                @Override
                public void fail(Throwable e) {
                    models.put(redis, new RedisRoleModel(redisMeta.getIp(), redisMeta.getPort(),e));
                }
            });
            futureMap.put(redis, redisRole);
        });

        for (Map.Entry<HostPort, CommandFuture<Role>> entry : futureMap.entrySet()) {
            try {
                entry.getValue().get();
            } catch (InterruptedException | ExecutionException e) {
                models.put(entry.getKey(), new RedisRoleModel(entry.getKey().getHost(), entry.getKey().getPort(), e));
            }
        }
        return new ArrayList<>(models.values());
    }

}
