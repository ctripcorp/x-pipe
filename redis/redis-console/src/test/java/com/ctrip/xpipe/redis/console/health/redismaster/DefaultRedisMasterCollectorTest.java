package com.ctrip.xpipe.redis.console.health.redismaster;

import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.health.RedisSession;
import com.ctrip.xpipe.redis.console.health.RedisSessionManager;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.impl.RedisServiceImpl;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.net.SocketException;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Feb 01, 2018
 */
public class DefaultRedisMasterCollectorTest {

    @Mock
    private RedisSessionManager redisSessionManager;

    @Mock
    private RedisSession redisSession;

    @Mock
    private RedisService redisService = spy(new RedisServiceImpl());

    @InjectMocks
    private DefaultRedisMasterCollector collector = new DefaultRedisMasterCollector();

    private RedisMasterSamplePlan plan;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        collector.postConstructDefaultRedisMasterCollector();

        plan = new RedisMasterSamplePlan("jq", "cluster1", "shard1");

        RedisMeta redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379).setMaster(XPipeConsoleConstant.DEFAULT_ADDRESS);
        plan.addRedis("jq", redisMeta, new InstanceRedisMasterResult());

        RedisMeta redisMeta2 = new RedisMeta().setIp("127.0.0.1").setPort(6380).setMaster(XPipeConsoleConstant.DEFAULT_ADDRESS);
        plan.addRedis("jq", redisMeta2, new InstanceRedisMasterResult());
    }

    @Test
    public void testDoCorrection() throws Exception {
        when(redisService.findAllByDcClusterShard(any(), any(), any())).then(new Answer<List<RedisTbl>> () {
            @Override
            public List<RedisTbl> answer(InvocationOnMock invocation) throws Throwable {
                return Lists.newArrayList(new RedisTbl().setRedisIp("127.0.0.1")
                        .setRedisPort(6379).setMaster(false), new RedisTbl().setRedisIp("127.0.0.1")
                        .setRedisPort(6380).setMaster(false));
            }
        });
        when(redisSessionManager.findOrCreateSession("127.0.0.1", 6380)).thenReturn(redisSession);
        when(redisSession.roleSync()).thenReturn("master");

        collector.doCorrection(plan);
        verify(redisService).updateBatchMaster(any());
    }

    @Test
    public void testIsMaster2() throws Exception {
        when(redisSessionManager.findOrCreateSession(any(), anyInt())).thenReturn(null);
        Assert.assertFalse(collector.isMaster("127.0.0.1", 6379));
    }

    @Test
    public void testIsMaster3() throws Exception {
        when(redisSession.roleSync()).thenThrow(new IllegalStateException("Redis not response"));
        when(redisSessionManager.findOrCreateSession(any(), anyInt())).thenReturn(redisSession);
        Assert.assertFalse(collector.isMaster("127.0.0.1", 6379));

    }

    @Test
    public void testIsMaster4() throws Exception {
        when(redisSession.roleSync()).thenReturn("slave");
        when(redisSessionManager.findOrCreateSession(any(), anyInt())).thenReturn(redisSession);
        Assert.assertFalse(collector.isMaster("127.0.0.1", 6379));
    }

    @Test
    public void testIsMaster5() throws Exception {
        when(redisSession.roleSync()).thenReturn("master");
        when(redisSessionManager.findOrCreateSession(any(), anyInt())).thenReturn(redisSession);
        Assert.assertTrue(collector.isMaster("127.0.0.1", 6379));
    }

    @Test
    public void testGeneratePlan() throws Exception {
        Assert.assertFalse(plan.isEmpty());
    }
}