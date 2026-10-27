package com.ctrip.xpipe.redis.comparator.balance;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * T-LB.4 / AC-15：全走注入的假 provider，不访问网络。
 */
public class CompareTaskAssignerTest extends AbstractTest {

    private static final String SOURCE =
            "src/main/java/com/ctrip/xpipe/redis/comparator/balance/CompareTaskAssigner.java";

    @Test
    public void testGroupNodesCoverAllDbIdsWithoutOverlap() {
        FakeServerGroupProvider provider = new FakeServerGroupProvider()
                .setCiCodes("host-b", "host-a", "host-c");
        ConfigurableCmsConfig config = ConfigurableCmsConfig.configured();
        CompareTaskAssigner a = assigner(provider, config, "host-a");
        CompareTaskAssigner b = assigner(provider, config, "host-b");
        CompareTaskAssigner c = assigner(provider, config, "host-c");
        a.refresh();
        b.refresh();
        c.refresh();
        Assert.assertEquals(3, a.getN());
        Assert.assertEquals(0, a.getIdx());
        Assert.assertEquals(1, b.getIdx());
        Assert.assertEquals(2, c.getIdx());

        for (long dbId = 0; dbId < 30; dbId++) {
            int owners = 0;
            if (a.isMine(dbId)) {
                owners++;
            }
            if (b.isMine(dbId)) {
                owners++;
            }
            if (c.isMine(dbId)) {
                owners++;
            }
            Assert.assertEquals("dbId=" + dbId, 1, owners);
            Assert.assertEquals(dbId % 3 == 0, a.isMine(dbId));
        }
    }

    @Test
    public void testUnconfiguredFallsBackToSingletonAndWarns() {
        FakeServerGroupProvider provider = new FakeServerGroupProvider()
                .fail(new RuntimeException("must not be called"));
        CompareTaskAssigner assigner = assigner(provider, new ComparatorConfig(), "local");
        assigner.refresh();
        Assert.assertEquals(1, assigner.getN());
        Assert.assertEquals(0, assigner.getIdx());
        Assert.assertTrue(assigner.isMine(0));
        Assert.assertTrue(assigner.isMine(7));
        Assert.assertEquals(1, assigner.getUnconfiguredWarnCount());
        Assert.assertEquals(0, provider.getListCalls());

        assigner.refresh();
        Assert.assertEquals(1, assigner.getUnconfiguredWarnCount());
    }

    @Test
    public void testUnreachableKeepsLastSuccess() {
        FakeServerGroupProvider provider = new FakeServerGroupProvider()
                .setCiCodes("other", "me");
        ConfigurableCmsConfig config = ConfigurableCmsConfig.configured();
        RecordingMonitor monitor = new RecordingMonitor();
        CompareTaskAssigner assigner = assigner(provider, config, "me", monitor);
        assigner.refresh();
        Assert.assertEquals(2, assigner.getN());
        Assert.assertEquals(0, assigner.getIdx());
        Assert.assertTrue(assigner.isMine(0));
        Assert.assertFalse(assigner.isMine(1));

        provider.fail(new RuntimeException("cms down"));
        assigner.refresh();
        Assert.assertEquals(2, assigner.getN());
        Assert.assertEquals(0, assigner.getIdx());
        Assert.assertTrue(assigner.isMine(0));
        Assert.assertFalse(assigner.isMine(1));
        Assert.assertTrue(monitor.events.contains(
                CompareTaskAssigner.MONITOR_TYPE + "/" + CompareTaskAssigner.EVENT_CMS_UNREACHABLE));
        Assert.assertFalse(monitor.events.contains(
                CompareTaskAssigner.MONITOR_TYPE + "/" + CompareTaskAssigner.EVENT_CMS_FIRST_FAIL));
    }

    @Test
    public void testFirstFetchFailureTakesNoShards() {
        FakeServerGroupProvider provider = new FakeServerGroupProvider()
                .fail(new RuntimeException("cms down"));
        RecordingMonitor monitor = new RecordingMonitor();
        CompareTaskAssigner assigner = assigner(provider, ConfigurableCmsConfig.configured(), "me", monitor);
        assigner.refresh();
        Assert.assertEquals(0, assigner.getN());
        Assert.assertEquals(-1, assigner.getIdx());
        Assert.assertFalse(assigner.isMine(0));
        Assert.assertFalse(assigner.isMine(1));
        Assert.assertTrue(monitor.events.contains(
                CompareTaskAssigner.MONITOR_TYPE + "/" + CompareTaskAssigner.EVENT_CMS_FIRST_FAIL));
        Assert.assertFalse(monitor.events.contains(
                CompareTaskAssigner.MONITOR_TYPE + "/" + CompareTaskAssigner.EVENT_CMS_UNREACHABLE));
    }

    @Test
    public void testFirstLabel() {
        Assert.assertEquals("r100011696-91040552-t6xl7",
                CompareTaskAssigner.firstLabel(
                        "r100011696-91040552-t6xl7.pro-captain.pod.share-shaalim-tcs-pro1.k8s.ctripcorp.com"));
        Assert.assertEquals("host-a", CompareTaskAssigner.firstLabel("host-a"));
        Assert.assertEquals("host-a", CompareTaskAssigner.firstLabel("  host-a.example.com  "));
        Assert.assertEquals("", CompareTaskAssigner.firstLabel(""));
        Assert.assertEquals("", CompareTaskAssigner.firstLabel(null));
    }

    @Test
    public void testFqdnCiCodeMatchesShortHostName() {
        FakeServerGroupProvider provider = new FakeServerGroupProvider()
                .setCiCodes(
                        "r100011696-91040552-8btvr.pro-captain.pod.share-shaalim-tcs-pro1.k8s.ctripcorp.com",
                        "r100011696-91040552-t6xl7.pro-captain.pod.share-shaalim-tcs-pro1.k8s.ctripcorp.com",
                        "r100011696-91040552-ftc8w.pro-captain.pod.share-shaalim-tcs-pro1.k8s.ctripcorp.com");
        CompareTaskAssigner assigner = assigner(provider, ConfigurableCmsConfig.configured(),
                "r100011696-91040552-t6xl7");
        assigner.refresh();
        Assert.assertEquals(3, assigner.getN());
        Assert.assertEquals(2, assigner.getIdx());
        Assert.assertTrue(assigner.isMine(2));
        Assert.assertFalse(assigner.isMine(0));
    }

    @Test
    public void testFqdnHostNameMatchesFqdnCiCode() {
        FakeServerGroupProvider provider = new FakeServerGroupProvider()
                .setCiCodes("r100011696-91040552-t6xl7.pro-captain.pod.share-shaalim-tcs-pro1.k8s.ctripcorp.com");
        CompareTaskAssigner assigner = assigner(provider, ConfigurableCmsConfig.configured(),
                "r100011696-91040552-t6xl7.pro-captain.pod.share-shaalim-tcs-pro1.k8s.ctripcorp.com");
        assigner.refresh();
        Assert.assertEquals(1, assigner.getN());
        Assert.assertEquals(0, assigner.getIdx());
        Assert.assertTrue(assigner.isMine(0));
    }

    @Test
    public void testAmbiguousFirstLabelTakesNoShards() {
        FakeServerGroupProvider provider = new FakeServerGroupProvider()
                .setCiCodes(
                        "r100011696-91040552-t6xl7.pro-captain.pod.share-shaalim-tcs-pro1.k8s.ctripcorp.com",
                        "r100011696-91040552-t6xl7.pro-captain.pod.share-shaalim-tcs-pro2.k8s.ctripcorp.com");
        RecordingMonitor monitor = new RecordingMonitor();
        CompareTaskAssigner assigner = assigner(provider, ConfigurableCmsConfig.configured(),
                "r100011696-91040552-t6xl7", monitor);
        assigner.refresh();
        Assert.assertEquals(2, assigner.getN());
        Assert.assertEquals(-1, assigner.getIdx());
        Assert.assertFalse(assigner.isMine(0));
        Assert.assertTrue(monitor.events.contains(
                CompareTaskAssigner.MONITOR_TYPE + "/" + CompareTaskAssigner.EVENT_HOST_NOT_IN_GROUP));
    }

    @Test
    public void testHostNotInListTakesNoShards() {
        FakeServerGroupProvider provider = new FakeServerGroupProvider()
                .setCiCodes("alpha", "beta");
        RecordingMonitor monitor = new RecordingMonitor();
        CompareTaskAssigner assigner = assigner(provider, ConfigurableCmsConfig.configured(), "me", monitor);
        assigner.refresh();
        Assert.assertEquals(2, assigner.getN());
        Assert.assertEquals(-1, assigner.getIdx());
        Assert.assertFalse(assigner.isMine(0));
        Assert.assertFalse(assigner.isMine(1));
        Assert.assertTrue(monitor.events.contains(
                CompareTaskAssigner.MONITOR_TYPE + "/" + CompareTaskAssigner.EVENT_HOST_NOT_IN_GROUP));
    }

    @Test
    public void testScaleOutReassigns() {
        FakeServerGroupProvider provider = new FakeServerGroupProvider()
                .setCiCodes("me", "other");
        CompareTaskAssigner assigner = assigner(provider, ConfigurableCmsConfig.configured(), "me");
        assigner.refresh();
        Assert.assertEquals(2, assigner.getN());
        Assert.assertEquals(0, assigner.getIdx());
        Assert.assertTrue(assigner.isMine(0));
        Assert.assertFalse(assigner.isMine(1));

        provider.setCiCodes("a", "me", "z");
        assigner.refresh();
        Assert.assertEquals(3, assigner.getN());
        Assert.assertEquals(1, assigner.getIdx());
        Assert.assertFalse(assigner.isMine(0));
        Assert.assertTrue(assigner.isMine(1));
        Assert.assertFalse(assigner.isMine(2));
    }

    @Test
    public void testRefreshThrowableDoesNotStopNextRound() {
        FakeServerGroupProvider provider = new FakeServerGroupProvider()
                .fail(new RuntimeException("boom"));
        CaptureFixedDelaySchedule capture = new CaptureFixedDelaySchedule();
        CompareTaskAssigner assigner = new CompareTaskAssigner(provider, ConfigurableCmsConfig.configured(),
                new StubFoundation("me"), capture, new RecordingMonitor());
        try {
            assigner.start();
            Assert.assertNotNull(capture.task.get());
            capture.task.get().run();
            Assert.assertFalse(assigner.isMine(0));

            provider.setCiCodes("me");
            capture.task.get().run();
            Assert.assertTrue(assigner.isMine(0));
            Assert.assertEquals(1, assigner.getN());
        } finally {
            assigner.stop();
            capture.shutdown();
        }
    }

    @Test
    public void testAssignerDoesNotCallNetwork() throws Exception {
        String text = new String(Files.readAllBytes(Paths.get(SOURCE)), StandardCharsets.UTF_8);
        Assert.assertFalse(text.contains("RestTemplate"));
        Assert.assertFalse(text.contains("RestTemplateFactory"));
        Assert.assertFalse(text.contains("postForObject"));
        Assert.assertTrue(text.contains("SCHEDULED_EXECUTOR"));
        Assert.assertTrue(text.contains("scheduleWithFixedDelay"));
    }

    private CompareTaskAssigner assigner(ServerGroupProvider provider, ComparatorConfig config, String host) {
        return assigner(provider, config, host, new RecordingMonitor());
    }

    private CompareTaskAssigner assigner(ServerGroupProvider provider, ComparatorConfig config, String host,
                                         EventMonitor monitor) {
        return new CompareTaskAssigner(provider, config, new StubFoundation(host), scheduled, monitor);
    }

    static final class ConfigurableCmsConfig extends ComparatorConfig {

        private final String token;

        private final String url;

        static ConfigurableCmsConfig configured() {
            return new ConfigurableCmsConfig("token", "http://cms.example/GetServer");
        }

        ConfigurableCmsConfig(String token, String url) {
            this.token = token;
            this.url = url;
        }

        @Override
        public String getCmsAccessToken() {
            return token;
        }

        @Override
        public String getCmsGetServerUrl() {
            return url;
        }
    }

    static final class StubFoundation implements FoundationService {

        private final String hostName;

        StubFoundation(String hostName) {
            this.hostName = hostName;
        }

        @Override
        public String getDataCenter() {
            return "jq";
        }

        @Override
        public String getAppId() {
            return "test";
        }

        @Override
        public String getLocalIp() {
            return "127.0.0.1";
        }

        @Override
        public String getHostName() {
            return hostName;
        }

        @Override
        public String getGroupId() {
            return "group1";
        }

        @Override
        public String getRegion() {
            return "sha";
        }

        @Override
        public int getOrder() {
            return 0;
        }
    }

    static final class CaptureFixedDelaySchedule extends ScheduledThreadPoolExecutor {

        final AtomicReference<Runnable> task = new AtomicReference<>();

        CaptureFixedDelaySchedule() {
            super(1);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
                                                         TimeUnit unit) {
            task.set(command);
            return super.schedule(() -> { }, 1, TimeUnit.HOURS);
        }
    }

    static class RecordingMonitor implements EventMonitor {

        final List<String> events = new ArrayList<>();

        private void add(String type, String name) {
            events.add(type + "/" + name);
        }

        @Override
        public void logEvent(String type, String name, long count) {
            add(type, name);
        }

        @Override
        public void logEvent(String type, String name) {
            add(type, name);
        }

        @Override
        public void logEvent(String type, String name, Map<String, String> nameValuePairs) {
            add(type, name);
        }

        @Override
        public void logError(String type, String name) {
            add(type, name);
        }

        @Override
        public void logError(String type, String name, Map<String, String> nameValuePairs) {
            add(type, name);
        }

        @Override
        public void logAlertEvent(String simpleAlertMessage) {
        }
    }
}
