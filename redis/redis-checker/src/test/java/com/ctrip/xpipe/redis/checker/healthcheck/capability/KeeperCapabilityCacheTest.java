package com.ctrip.xpipe.redis.checker.healthcheck.capability;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractConfigCommand.REDIS_CONFIG_TYPE;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KeeperCapabilityCacheTest {

    private static final String PREPARE_WATCH = REDIS_CONFIG_TYPE.PREPARE_WATCH.getConfigName();

    private static final String PUBSUB_PARSE = REDIS_CONFIG_TYPE.PUBSUB_PARSE.getConfigName();

    @Test
    public void testCapabilityCombinations() {
        boolean[][] combinations = {{true, true}, {true, false}, {false, true}, {false, false}};
        for (int i = 0; i < combinations.length; i++) {
            KeeperCapabilityCache cache = new KeeperCapabilityCache();
            ControlledSession session = new ControlledSession();
            HostPort address = new HostPort("127.0.0." + (i + 1), 6380);

            cache.refresh(instance(address, "dc", session));
            session.success(PREPARE_WATCH, combinations[i][0] ? "1" : "0");
            session.success(PUBSUB_PARSE, combinations[i][1] ? "1" : "0");

            Assert.assertEquals(combinations[i][0] && combinations[i][1]
                            ? KeeperCapabilityCache.Capability.SUPPORTED
                            : KeeperCapabilityCache.Capability.UNSUPPORTED,
                    cache.get(address));
        }
    }

    @Test
    public void testUnknownAndFailurePreservesLastKnownGood() {
        KeeperCapabilityCache cache = new KeeperCapabilityCache();
        ControlledSession session = new ControlledSession();
        HostPort address = new HostPort("127.0.0.1", 6380);
        KeeperHealthCheckInstance instance = instance(address, "dc", session);

        Assert.assertEquals(KeeperCapabilityCache.Capability.UNKNOWN, cache.get(address));
        cache.refresh(instance);
        session.success(PREPARE_WATCH, "1");
        session.fail(PUBSUB_PARSE, new IllegalStateException("first refresh failed"));
        Assert.assertEquals(KeeperCapabilityCache.Capability.UNKNOWN, cache.get(address));

        cache.refresh(instance);
        session.success(PREPARE_WATCH, "1");
        session.success(PUBSUB_PARSE, "1");
        Assert.assertEquals(KeeperCapabilityCache.Capability.SUPPORTED, cache.get(address));

        cache.refresh(instance);
        session.fail(PREPARE_WATCH, new IllegalStateException("retry failed"));
        session.success(PUBSUB_PARSE, "0");
        Assert.assertEquals(KeeperCapabilityCache.Capability.SUPPORTED, cache.get(address));
    }

    @Test
    public void testConcurrentRefreshIsDeduplicated() throws Exception {
        KeeperCapabilityCache cache = new KeeperCapabilityCache();
        ControlledSession session = new ControlledSession();
        KeeperHealthCheckInstance instance = instance(new HostPort("127.0.0.1", 6380), "dc", session);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        try {
            for (int i = 0; i < 32; i++) {
                executor.submit(() -> {
                    start.await();
                    cache.refresh(instance);
                    return null;
                });
            }
            start.countDown();
            executor.shutdown();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

            Assert.assertEquals(1, session.count(PREPARE_WATCH));
            Assert.assertEquals(1, session.count(PUBSUB_PARSE));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testInvalidateByAddressAndDc() {
        KeeperCapabilityCache cache = new KeeperCapabilityCache();
        HostPort first = new HostPort("127.0.0.1", 6380);
        HostPort second = new HostPort("127.0.0.2", 6380);
        HostPort otherDc = new HostPort("127.0.0.3", 6380);
        refresh(cache, first, "dc1", "1", "1");
        refresh(cache, second, "dc1", "1", "0");
        refresh(cache, otherDc, "dc2", "1", "1");

        cache.invalidate(first);
        Assert.assertEquals(KeeperCapabilityCache.Capability.UNKNOWN, cache.get(first));
        Assert.assertEquals(KeeperCapabilityCache.Capability.UNSUPPORTED, cache.get(second));

        cache.invalidateDc("dc1");
        Assert.assertEquals(KeeperCapabilityCache.Capability.UNKNOWN, cache.get(second));
        Assert.assertEquals(KeeperCapabilityCache.Capability.SUPPORTED, cache.get(otherDc));
    }

    @Test
    public void testStaleCallbacksCannotWriteAfterAddressReuseOrDcStop() {
        KeeperCapabilityCache cache = new KeeperCapabilityCache();
        ControlledSession session = new ControlledSession();
        HostPort address = new HostPort("127.0.0.1", 6380);
        KeeperHealthCheckInstance instance = instance(address, "dc1", session);

        cache.refresh(instance);
        cache.invalidate(address);
        cache.refresh(instance);
        session.successAt(PREPARE_WATCH, 1, "1");
        session.successAt(PUBSUB_PARSE, 1, "1");
        session.success(PREPARE_WATCH, "0");
        session.success(PUBSUB_PARSE, "0");
        Assert.assertEquals(KeeperCapabilityCache.Capability.SUPPORTED, cache.get(address));

        HostPort stopped = new HostPort("127.0.0.2", 6380);
        ControlledSession stoppedSession = new ControlledSession();
        cache.refresh(instance(stopped, "dc-stop", stoppedSession));
        cache.invalidateDc("dc-stop");
        stoppedSession.success(PREPARE_WATCH, "1");
        stoppedSession.success(PUBSUB_PARSE, "1");
        Assert.assertEquals(KeeperCapabilityCache.Capability.UNKNOWN, cache.get(stopped));
    }

    @Test
    public void testInvalidateAllFencesOldCallbacksAndDoesNotClearReplacementInflight() {
        KeeperCapabilityCache cache = new KeeperCapabilityCache();
        ControlledSession session = new ControlledSession();
        HostPort address = new HostPort("127.0.0.1", 6380);
        KeeperHealthCheckInstance instance = instance(address, "dc", session);

        cache.refresh(instance);
        cache.invalidateAll();
        cache.refresh(instance);
        session.success(PREPARE_WATCH, "1");
        session.success(PUBSUB_PARSE, "1");
        Assert.assertEquals(KeeperCapabilityCache.Capability.UNKNOWN, cache.get(address));

        session.success(PREPARE_WATCH, "1");
        session.success(PUBSUB_PARSE, "1");
        Assert.assertEquals(KeeperCapabilityCache.Capability.SUPPORTED, cache.get(address));
    }

    @Test
    public void testSynchronousFailureIsContainedAndReadPathDoesNotQuery() throws Exception {
        KeeperCapabilityCache cache = new KeeperCapabilityCache();
        ControlledSession session = new ControlledSession();
        HostPort address = new HostPort("127.0.0.1", 6380);
        session.throwOn(PREPARE_WATCH);

        cache.refresh(instance(address, "dc", session));
        session.success(PUBSUB_PARSE, "1");
        Assert.assertEquals(KeeperCapabilityCache.Capability.UNKNOWN, cache.get(address));
        Assert.assertEquals(KeeperCapabilityCache.Capability.UNKNOWN, cache.getIfPresent(address));
        Assert.assertEquals(1, session.count(PREPARE_WATCH));
        Assert.assertEquals(1, session.count(PUBSUB_PARSE));

        String source = source();
        String getBody = source.substring(source.indexOf("public Capability get("),
                source.indexOf("public Capability getIfPresent("));
        String getIfPresentBody = source.substring(source.indexOf("public Capability getIfPresent("),
                source.indexOf("public void refresh("));
        Assert.assertFalse(getBody.contains("ConfigGet"));
        Assert.assertFalse(getBody.contains("refresh("));
        Assert.assertFalse(getIfPresentBody.contains("ConfigGet"));
        Assert.assertFalse(getIfPresentBody.contains("refresh("));
        Assert.assertTrue(source.contains("extends AbstractCommand<Boolean>"));
        Assert.assertTrue(source.contains("new ParallelCommandChain(MoreExecutors.directExecutor(), false)"));
        Assert.assertFalse(source.contains("AtomicInteger"));
        Assert.assertFalse(source.contains("DefaultDcMetaChangeManager"));
        Assert.assertFalse(source.contains("KeeperDelayAction"));
    }

    private void refresh(KeeperCapabilityCache cache, HostPort address, String dcId,
                         String prepareWatch, String pubsubParse) {
        ControlledSession session = new ControlledSession();
        cache.refresh(instance(address, dcId, session));
        session.success(PREPARE_WATCH, prepareWatch);
        session.success(PUBSUB_PARSE, pubsubParse);
    }

    private KeeperHealthCheckInstance instance(HostPort address, String dcId, RedisSession session) {
        KeeperInstanceInfo info = Mockito.mock(KeeperInstanceInfo.class);
        Mockito.when(info.getHostPort()).thenReturn(address);
        Mockito.when(info.getDcId()).thenReturn(dcId);
        KeeperHealthCheckInstance instance = Mockito.mock(KeeperHealthCheckInstance.class);
        Mockito.when(instance.getCheckInfo()).thenReturn(info);
        Mockito.when(instance.getRedisSession()).thenReturn(session);
        return instance;
    }

    private String source() throws Exception {
        File file = new File("src/main/java/com/ctrip/xpipe/redis/checker/healthcheck/capability/"
                + "KeeperCapabilityCache.java");
        Assert.assertTrue("source missing: " + file.getAbsolutePath(), file.isFile());
        return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
    }

    private static final class ControlledSession extends RedisSession {

        private final Map<String, List<Callbackable<String>>> callbacks = new HashMap<>();

        private final Map<String, Integer> counts = new HashMap<>();

        private String throwingKey;

        @Override
        public synchronized void ConfigGet(Callbackable<String> callback, String args) {
            counts.put(args, count(args) + 1);
            if (args.equals(throwingKey)) {
                throw new IllegalStateException("synchronous CONFIG GET failure");
            }
            callbacks.computeIfAbsent(args, key -> new ArrayList<>()).add(callback);
        }

        private synchronized int count(String key) {
            return counts.getOrDefault(key, 0);
        }

        private synchronized void throwOn(String key) {
            throwingKey = key;
        }

        private void success(String key, String value) {
            successAt(key, 0, value);
        }

        private void successAt(String key, int index, String value) {
            take(key, index).success(value);
        }

        private void fail(String key, Throwable throwable) {
            take(key, 0).fail(throwable);
        }

        private synchronized Callbackable<String> take(String key, int index) {
            List<Callbackable<String>> pending = callbacks.get(key);
            Assert.assertNotNull("no callback for " + key, pending);
            Assert.assertTrue("no callback " + index + " for " + key, pending.size() > index);
            return pending.remove(index);
        }
    }
}
