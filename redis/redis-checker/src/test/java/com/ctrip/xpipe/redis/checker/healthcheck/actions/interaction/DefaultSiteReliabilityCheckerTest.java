//package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;
//
//import com.ctrip.xpipe.cluster.ClusterType;
//import com.ctrip.xpipe.endpoint.HostPort;
//import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
//import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
//import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
//import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceDown;
//import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceSick;
//import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
//import com.ctrip.xpipe.redis.core.AbstractRedisTest;
//import com.ctrip.xpipe.redis.core.meta.MetaCache;
//import com.google.common.collect.Lists;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Ignore;
//import org.junit.Test;
//import org.mockito.InjectMocks;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//import org.mockito.invocation.InvocationOnMock;
//import org.mockito.stubbing.Answer;
//
//import java.util.List;
//import java.util.Random;
//
//import static org.mockito.Matchers.*;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
///**
// * @author chen.zhu
// * <p>
// * Sep 18, 2018
// */
//public class DefaultSiteReliabilityCheckerTest extends AbstractRedisTest {
//
//    @InjectMocks
//    private DefaultSiteReliabilityChecker checker = new DefaultSiteReliabilityChecker();
//
//    @Mock
//    private MetaCache metaCache;
//
//    @Mock
//    private DefaultDelayPingActionCollector defaultDelayPingActionCollector;
//
//    @Mock
//    private CheckerConfig checkerConfig;
//
//    private RedisHealthCheckInstance instance;
//
//    private RedisInstanceInfo info;
//
//    private Random random = new Random();
//
//    @Before
//    public void beforeDefaultSiteReliabilityCheckerTest() {
//        MockitoAnnotations.initMocks(this);
//        instance = mock(RedisHealthCheckInstance.class);
//        info = new DefaultRedisInstanceInfo("dc", "cluster", "shard", new HostPort("localhost", 1234), "dc", ClusterType.ONE_WAY);
//        when(instance.getCheckInfo()).thenReturn(info);
//        when(metaCache.getAllActiveRedisOfDc(anyString(), anyString())).thenReturn(Lists.newArrayList(info.getHostPort(), new HostPort("localhost", 5678)));
//    }
//
//    @Test
//    public void testCheckWithSiteDown() throws Exception {
//
//        when(defaultDelayPingActionCollector.getState(any(HostPort.class))).thenReturn(HEALTH_STATE.DOWN);
//        boolean result = checker.isSiteHealthy(new InstanceDown(instance));
//        Assert.assertFalse(result);
//
//    }
//
//    @Test
//    public void testConfigSetConsoleSiteUnstable() {
//        when(defaultDelayPingActionCollector.getState(any(HostPort.class))).thenReturn(HEALTH_STATE.INSTANCEUP);
//        when(checkerConfig.isConsoleSiteUnstable()).thenReturn(true).thenReturn(false);
//        Assert.assertFalse(checker.isSiteHealthy(new InstanceDown(instance)));
//        Assert.assertTrue(checker.isSiteHealthy(new InstanceDown(instance)));
//    }
//
//    @Test
//    public void testCheckWithSiteOk() {
//        when(defaultDelayPingActionCollector.getState(any(HostPort.class))).thenReturn(HEALTH_STATE.HEALTHY);
//        Assert.assertTrue(checker.isSiteHealthy(new InstanceDown(instance)));
//    }
//
//    @Test
//    public void testCheckMultipleEventsDown() throws Exception {
//        int N = 100;
//        List<HostPort> dcHostPort = hostPorts(N);
//        when(defaultDelayPingActionCollector.getState(any(HostPort.class))).thenAnswer(new Answer<HEALTH_STATE>() {
//            @Override
//            public HEALTH_STATE answer(InvocationOnMock invocation) throws Throwable {
//                HostPort hostPort = invocation.getArgument(0, HostPort.class);
//                if(dcHostPort.contains(hostPort)) {
//                    return HEALTH_STATE.DOWN;
//                }
//                return HEALTH_STATE.HEALTHY;
//            }
//        });
//        when(metaCache.getAllActiveRedisOfDc(anyString(), eq("dc"))).thenReturn(dcHostPort);
//        checker.setMetaCache(metaCache);
//        boolean result;
//        for(int i = 0; i < N/2 - 1; i++) {
//            if((random.nextInt() & 1) == 1) {
//                result = checker.isSiteHealthy(new InstanceDown(relatedInstance(dcHostPort.get(randomInt(0, dcHostPort.size() -1 )))));
//            } else {
//                result = checker.isSiteHealthy(new InstanceSick(relatedInstance(dcHostPort.get(randomInt(0, dcHostPort.size() -1 )))));
//            }
//            Assert.assertFalse(result);
//        }
//        RedisHealthCheckInstance instance = mock(RedisHealthCheckInstance.class);
//        RedisInstanceInfo info = new DefaultRedisInstanceInfo("dc2", "cluster", "shard", localHostport(randomPort()), "dc2", ClusterType.ONE_WAY);
//        when(instance.getCheckInfo()).thenReturn(info);
//        result = checker.isSiteHealthy(new InstanceDown(instance));
//        Assert.assertTrue(result);
//    }
//
//    private List<HostPort> hostPorts(int num) {
//        List<HostPort> result = Lists.newArrayList();
//        while(num -- > 0) {
//            result.add(localHostport(randomPort()));
//        }
//        return result;
//    }
//
//    private RedisHealthCheckInstance relatedInstance(HostPort hostPort) {
//        RedisHealthCheckInstance instance = mock(RedisHealthCheckInstance.class);
//        RedisInstanceInfo info = new DefaultRedisInstanceInfo("dc", "cluster", "shard", hostPort, "dc", ClusterType.ONE_WAY);
//        when(instance.getCheckInfo()).thenReturn(info);
//        return instance;
//    }
//}