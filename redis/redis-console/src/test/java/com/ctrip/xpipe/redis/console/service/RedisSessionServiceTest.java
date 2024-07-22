package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.model.RedisRoleModel;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisConsoleSessionManager;
import com.ctrip.xpipe.redis.console.service.impl.RedisSessionServiceImpl;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class RedisSessionServiceTest {

    @Mock
    private RedisConsoleSessionManager sessionManager;

    @Mock
    private MetaCache metaCache;

    @InjectMocks
    private RedisSessionServiceImpl redisSessionService;

    public static final String DC = "jq";

    public static final String CLUSTER = "cluster";

    public static final String SHARD = "shard";

    public static final String IP = "127.0.0.1";

    public static final int PORT = 6379;

    @Test
    public void testGetRedisRole() {
        RedisSession session = Mockito.mock(RedisSession.class);
        CommandFuture future = Mockito.mock(CommandFuture.class);
        RedisSession.RollCallback callback = new RedisSession.RollCallback() {
            @Override
            public void role(String role, Role detail) {

            }

            @Override
            public void fail(Throwable e) {

            }
        };
        Mockito.when(sessionManager.findOrCreateSession(new DefaultEndPoint(IP, PORT))).thenReturn(session);
        Mockito.when(session.role(callback)).thenReturn(future);
        CommandFuture<Role> redisRole = redisSessionService.getRedisRole(IP, PORT, callback);
        Assert.assertEquals(redisRole, future);
    }

    @Test
    public void testGetShardAllRedisRole() throws Exception{
        List<RedisMeta> redisMetas = new ArrayList<>();
        RedisMeta redisMeta = Mockito.mock(RedisMeta.class);
        redisMetas.add(redisMeta);
        Mockito.when(redisMeta.getIp()).thenReturn(IP);
        Mockito.when(redisMeta.getPort()).thenReturn(PORT);
        Mockito.when(metaCache.getRedisOfDcClusterShard(DC, CLUSTER, SHARD)).thenReturn(redisMetas);
        RedisSession session = Mockito.mock(RedisSession.class);
        Mockito.when(sessionManager.findOrCreateSession(new DefaultEndPoint(IP, PORT))).thenReturn(session);
        CommandFuture future = Mockito.mock(CommandFuture.class);
        Mockito.when(session.role(any())).thenReturn(future);
        InterruptedException exception = Mockito.mock(InterruptedException.class);
        Mockito.when(future.get()).thenThrow(exception);
        List<RedisRoleModel> shardAllRedisRole = redisSessionService.getShardAllRedisRole(DC, CLUSTER, SHARD);
        Assert.assertEquals(shardAllRedisRole.size(), 1);
        Assert.assertEquals(shardAllRedisRole.get(0).getHost(), IP);
        Assert.assertEquals(shardAllRedisRole.get(0).getPort(), PORT);
        Assert.assertEquals(shardAllRedisRole.get(0).getErr(), exception);
    }


}
