package com.ctrip.xpipe.redis.console.healthcheck.action;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.action.event.InstanceDown;
import com.ctrip.xpipe.redis.console.healthcheck.action.event.InstanceSick;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Sep 18, 2018
 */
public class DefaultSiteReliabilityCheckerTest {

    private DefaultSiteReliabilityChecker checker;

    @Mock
    private MetaCache metaCache;

    private RedisHealthCheckInstance instance;

    private Random random = new Random();

    @Before
    public void beforeDefaultSiteReliabilityCheckerTest() {
        MockitoAnnotations.initMocks(this);
        checker = new DefaultSiteReliabilityChecker() {
            @Override
            protected int getCheckInterval() {
                return 1;
            }
        };
        checker.setMetaCache(metaCache);
        checker.setScheduled(Executors.newScheduledThreadPool(1));
        instance = mock(RedisHealthCheckInstance.class);
        RedisInstanceInfo info = mock(RedisInstanceInfo.class);
        when(instance.getRedisInstanceInfo()).thenReturn(info);
        when(info.getDcId()).thenReturn("dc");
    }

    @Test
    public void testCheckWithSiteDown() throws ExecutionException, InterruptedException {
        when(metaCache.getRedisNumOfDc(anyString())).thenReturn(1);
        CommandFuture<Boolean> future = checker.check(new InstanceDown(instance));
        Assert.assertFalse(future.get());
    }

    @Test
    public void testCheckWithSiteOk() throws Exception {
        when(metaCache.getRedisNumOfDc(anyString())).thenReturn(100);
        CommandFuture<Boolean> future = checker.check(new InstanceDown(instance));
        Assert.assertTrue(future.get());
    }

    @Test
    public void testCheckMultipleEventsDown() throws Exception {
        int N = 100;
        when(metaCache.getRedisNumOfDc(anyString())).thenReturn(N);
        checker = new DefaultSiteReliabilityChecker() {
            @Override
            protected int getCheckInterval() {
                return 300;
            }
        };
        checker.setMetaCache(metaCache);
        checker.setScheduled(Executors.newScheduledThreadPool(1));
        for(int i = 0; i < N/2 - 1; i++) {
            if((random.nextInt() & 1) == 1) {
                checker.check(new InstanceDown(instance));
            } else {
                checker.check(new InstanceSick(instance));
            }
        }
        CommandFuture<Boolean> future = checker.check(new InstanceSick(instance));
        Assert.assertFalse(future.get());
    }

    @Test
    public void testCheckMultipleEventsOk() throws Exception {
        int N = 100;
        when(metaCache.getRedisNumOfDc(anyString())).thenReturn(N);
        checker = new DefaultSiteReliabilityChecker() {
            @Override
            protected int getCheckInterval() {
                return 300;
            }
        };
        checker.setMetaCache(metaCache);
        checker.setScheduled(Executors.newScheduledThreadPool(1));
        for(int i = 0; i < N/4 - 1; i++) {
            if((random.nextInt() & 1) == 1) {
                checker.check(new InstanceDown(instance));
            } else {
                checker.check(new InstanceSick(instance));
            }
        }
        CommandFuture<Boolean> future = checker.check(new InstanceSick(instance));
        Assert.assertTrue(future.get());
    }
}