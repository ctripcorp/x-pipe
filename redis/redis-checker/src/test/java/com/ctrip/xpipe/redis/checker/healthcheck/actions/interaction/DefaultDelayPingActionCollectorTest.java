package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.ClusterHealthManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    public void testProblematicBehavior() throws InterruptedException {
        Map<String, String> map = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(2);

        Thread t1 = new Thread(() -> {
            getOrCreate(map, "key1", "value1");
            latch.countDown();
        });

        Thread t2 = new Thread(() -> {
            remove(map, "key1");
            latch.countDown();
        });

        t1.start();
        t2.start();
        latch.await();
        Assert.assertNotNull(map.get("key1"));
    }

    @Test
    public void testSynchronizedBehavior() throws InterruptedException {
        Map<String, String> map = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(2);

        Thread t1 = new Thread(() -> {
            getOrCreate(map, "key1", "value1");
            latch.countDown();
        });

        Thread t2 = new Thread(() -> {
            removeSynchronized(map, "key1");
            latch.countDown();
        });

        t1.start();
        t2.start();
        latch.await();
        Assert.assertNull(map.get("key1"));
    }

    @Test
    @Ignore
    public void testComputeIfAbsent() throws InterruptedException {
        Map<String, String> map = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(2);

        Thread t1 = new Thread(() -> {
            getOrCreateComputeIfAbsent(map, "key1", "value1");
            latch.countDown();
        });

        Thread t2 = new Thread(() -> {
            remove(map, "key1");
            latch.countDown();
        });

        t1.start();
        t2.start();
        latch.await();
        Assert.assertNull(map.get("key1"));
    }


    public <K, V>  V getOrCreate(Map<K, V> map, K key, V val){

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

    public void getOrCreateComputeIfAbsent(Map<String, String> map, String key, String val){
        map.computeIfAbsent(key, k -> val);
        System.out.println("create: " + key);
    }

    public void remove(Map<String, String> map, String key) {
        map.remove(key);
        System.out.println("remove: " + key);
    }

    public void removeSynchronized(Map<String, String> map, String key) {
        synchronized (map) {
            System.out.println("remove: get lock");
            map.remove(key);
        }
        System.out.println("remove: " + key);
    }

}
