package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.ClusterHealthManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.mockito.Mockito.when;

/**
 * @author lishanglin
 * date 2024/10/15
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultDelayPingActionCollectorTest extends AbstractCheckerTest {

    @InjectMocks
    private DefaultDelayPingActionCollector delayPingActionCollector;

    @Mock
    private ClusterHealthManager clusterHealthManager;

    @Mock
    private CheckerConfig config;

    @Mock
    private Observer observer;

    private RedisHealthCheckInstance instance;

    private int lastMarkTimeout = 20;

    protected Map<String, String> map = Maps.newConcurrentMap();
    protected Set<String> set = Sets.newHashSet();


    @Before
    public void setupDefaultDelayPingActionCollectorTest() throws Exception {
        delayPingActionCollector.setScheduled(scheduled);
        when(clusterHealthManager.createHealthStatusObserver()).thenReturn(observer);

        instance = newRandomRedisHealthCheckInstance(6379);
        delayPingActionCollector.createOrGetHealthStatus(instance);

        when(config.getMarkdownInstanceMaxDelayMilli()).thenReturn(lastMarkTimeout/2);
        when(config.getCheckerMetaRefreshIntervalMilli()).thenReturn(lastMarkTimeout/2);
    }

    @Test
    public void setGetLastMarkTest() {
        HealthStatusDesc statusDesc = delayPingActionCollector.getHealthStatusDesc(new HostPort("10.0.0.1", 10));
        Assert.assertEquals(HEALTH_STATE.UNKNOWN, statusDesc.getState());
        Assert.assertNull(statusDesc.getLastMarkHandled());

        HostPort hostPort = new HostPort(instance.getEndpoint().getHost(), instance.getEndpoint().getPort());
        delayPingActionCollector.updateLastMarkHandled(hostPort, true);
        statusDesc = delayPingActionCollector.getHealthStatusDesc(hostPort);
        Assert.assertTrue(statusDesc.getLastMarkHandled());

        sleep(lastMarkTimeout + 1);
        statusDesc = delayPingActionCollector.getHealthStatusDesc(hostPort);
        Assert.assertNull(statusDesc.getLastMarkHandled());
    }

    @Test
    public void testCreateAfterRemove() throws InterruptedException {
        map.clear();
        CountDownLatch latch = new CountDownLatch(2);

        Thread t1 = new Thread(() -> {
            getOrCreate(map, "key1", "value1");
            latch.countDown();
        });

        Thread t2 = new Thread(() -> {
            remove("key1");
            latch.countDown();
        });

        t1.start();
        t2.start();
        latch.await();
        Assert.assertNotNull(map.get("key1"));
    }

    @Test
    public void testCreateAfterRemoveSynchronized() throws InterruptedException {
        map.clear();
        CountDownLatch latch = new CountDownLatch(2);

        Thread t1 = new Thread(() -> {
            createOrGet("key1", "value1");
            latch.countDown();
        });

        Thread t2 = new Thread(() -> {
            removeSynchronized("key1");
            latch.countDown();
        });

        t1.start();
        t2.start();
        latch.await();
        Assert.assertNull(map.get("key1"));
    }

    @Test
    public void testRemoveBeforeGet() throws InterruptedException {
        map.clear();
        CountDownLatch latch = new CountDownLatch(2);

        Thread t1 = new Thread(() -> {
            removeSynchronized("key1");
            latch.countDown();
        });

        Thread t2 = new Thread(() -> {
            createOrGet("key1", "value1");
            latch.countDown();
        });

        set.add("key1");

        t1.start();
        t2.start();
        latch.await();
        Assert.assertNull(map.get("key1"));
    }

    private synchronized String createOrGet(String key, String value) {
        if (!set.contains(key)) {
            return null;
        }
        if (map.containsKey(key)) {
            return map.get(key);
        } else {
            return getOrCreate(map, key, value);
        }
    }

    private  <K, V>  V getOrCreate(Map<K, V> map, K key, V val){

        V value = map.get(key);
        if(value != null){
            return value;
        }

        synchronized (map) {
            System.out.println("create: get lock");
            sleep(10);
            value = map.get(key);
            if(value == null){
                value = val;
                map.put(key, value);
            }
        }
        System.out.println("create: release lock");
        return value;
    }

    private void remove(String key) {
        map.remove(key);
        System.out.println("remove: " + key);
    }

    private synchronized void removeSynchronized(String key) {
        System.out.println("remove: get lock");
        set.remove(key);
        map.remove(key);
        System.out.println("remove: " + key);
    }

}
