package com.ctrip.xpipe.redis.comparator.meta;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.redis.comparator.balance.CompareTaskAssigner;
import com.ctrip.xpipe.redis.comparator.balance.FakeServerGroupProvider;
import com.ctrip.xpipe.redis.comparator.compare.CompareLane;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.stream.StreamRingBuffer;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * T-MT.4 / AC-14 / AC-14b ⑤：假 Meta / 假协商 / 假流，不访问网络。
 */
public class ShardCompareTaskManagerTest extends AbstractTest {

    private static final String SOURCE =
            "src/main/java/com/ctrip/xpipe/redis/comparator/meta/ShardCompareTaskManager.java";

    private static final long MINE = 10L;

    private static final long OTHER = 11L;

    @Test
    public void testOnlyBuildsTaskWhenAtLeastTwoTfsKeepers() throws Exception {
        StubMeta meta = new StubMeta();
        ClusterMeta cluster = new ClusterMeta("c1");
        addShard(meta.dc, cluster, MINE, "two-tfs",
                keeper("tfs", "10.0.0.1", 6380),
                keeper("tfs", "10.0.0.2", 6380));
        addShard(meta.dc, cluster, 12L, "one-tfs",
                keeper("tfs", "10.0.0.3", 6380),
                keeper("DEFAULT", "10.0.0.4", 6380));
        meta.dc.addCluster(cluster);

        FakeFactory factory = new FakeFactory();
        ShardCompareTaskManager manager = manager(meta, allReady(), factory, allMineAssigner());
        manager.refresh();

        Assert.assertEquals(1, manager.getTasks().size());
        Assert.assertNotNull(manager.taskOf(MINE));
        Assert.assertNull(manager.taskOf(12L));
        Assert.assertEquals(2, factory.opens.size());
        Assert.assertTrue(factory.opens.contains("10.0.0.1:6380"));
        Assert.assertTrue(factory.opens.contains("10.0.0.2:6380"));
    }

    @Test
    public void testInsufficientLanesDoesNotOpenSingleStream() throws Exception {
        StubMeta meta = twoTfsMine();
        FakeWatchCache cache = new FakeWatchCache();
        cache.put("10.0.0.1:6380", true);
        cache.put("10.0.0.2:6380", false);
        FakeFactory factory = new FakeFactory();
        RecordingMonitor monitor = new RecordingMonitor();
        ShardCompareTaskManager manager = manager(meta, cache, factory, allMineAssigner(), monitor);
        manager.refresh();

        Assert.assertTrue(manager.getTasks().isEmpty());
        Assert.assertEquals(0, factory.opens.size());
        Assert.assertTrue(monitor.events.contains(
                ShardCompareTaskManager.MONITOR_TYPE + "/" + ShardCompareTaskManager.EVENT_INSUFFICIENT_LANES));
    }

    @Test
    public void testSameDcMetaDoesNotRebuildStreams() throws Exception {
        StubMeta meta = twoTfsMine();
        FakeFactory factory = new FakeFactory();
        ShardCompareTaskManager manager = manager(meta, allReady(), factory, allMineAssigner());
        manager.refresh();
        CompareLane first = manager.taskOf(MINE).getStreams().get("10.0.0.1:6380");
        CompareLane second = manager.taskOf(MINE).getStreams().get("10.0.0.2:6380");
        Assert.assertEquals(2, factory.opens.size());

        manager.refresh();
        Assert.assertEquals(2, factory.opens.size());
        Assert.assertEquals(0, factory.closes.size());
        Assert.assertTrue(factory.releases.isEmpty());
        Assert.assertSame(first, manager.taskOf(MINE).getStreams().get("10.0.0.1:6380"));
        Assert.assertSame(second, manager.taskOf(MINE).getStreams().get("10.0.0.2:6380"));
    }

    @Test
    public void testPortChangeKeeperDiffAndNotMine() throws Exception {
        StubMeta meta = twoTfsMine();
        FakeFactory factory = new FakeFactory();
        FakeWatchCache cache = allReady();
        FakeServerGroupProvider provider = new FakeServerGroupProvider().setCiCodes("me", "other");
        CaptureFixedDelaySchedule assignerSched = new CaptureFixedDelaySchedule();
        CompareTaskAssigner assigner = new CompareTaskAssigner(provider, new CmsConfig(),
                new StubFoundation("me"), assignerSched, new RecordingMonitor());
        assigner.start();
        assignerSched.task.get().run();
        ShardCompareTaskManager manager = manager(meta, cache, factory, assigner);
        manager.refresh();
        Assert.assertEquals(2, factory.opens.size());
        CompareLane kept = manager.taskOf(MINE).getStreams().get("10.0.0.1:6380");

        meta.dc = new DcMeta("jq");
        ClusterMeta cluster = new ClusterMeta("c1");
        addShard(meta.dc, cluster, MINE, "two-tfs",
                keeper("tfs", "10.0.0.1", 6380),
                keeper("tfs", "10.0.0.2", 6381),
                keeper("tfs", "10.0.0.3", 6380));
        addShard(meta.dc, cluster, OTHER, "other",
                keeper("tfs", "10.0.1.1", 6380),
                keeper("tfs", "10.0.1.2", 6380));
        meta.dc.addCluster(cluster);
        manager.refresh();

        Assert.assertNull(manager.taskOf(OTHER));
        Assert.assertNotNull(manager.taskOf(MINE));
        Assert.assertSame(kept, manager.taskOf(MINE).getStreams().get("10.0.0.1:6380"));
        Assert.assertTrue(factory.closes.contains("10.0.0.2:6380"));
        Assert.assertTrue(factory.opens.contains("10.0.0.2:6381"));
        Assert.assertTrue(factory.opens.contains("10.0.0.3:6380"));
        Assert.assertFalse(factory.opens.contains("10.0.1.1:6380"));
        Assert.assertTrue(factory.releases.isEmpty());

        provider.setCiCodes("other");
        assignerSched.task.get().run();
        manager.refresh();
        Assert.assertTrue(manager.getTasks().isEmpty());
        Assert.assertTrue(factory.closes.contains("10.0.0.1:6380"));
        Assert.assertEquals(Collections.singletonList(MINE), factory.releases);
        assigner.stop();
        assignerSched.shutdown();
    }

    @Test
    public void testReadyLanesDropToOneStopsExistingTask() throws Exception {
        StubMeta meta = twoTfsMine();
        FakeWatchCache cache = allReady();
        FakeFactory factory = new FakeFactory();
        RecordingMonitor monitor = new RecordingMonitor();
        ShardCompareTaskManager manager = manager(meta, cache, factory, allMineAssigner(), monitor);
        manager.refresh();
        Assert.assertNotNull(manager.taskOf(MINE));
        Assert.assertEquals(2, factory.opens.size());

        cache.defaultReady = false;
        cache.put("10.0.0.1:6380", true);
        cache.put("10.0.0.2:6380", false);
        manager.refresh();

        Assert.assertNull(manager.taskOf(MINE));
        Assert.assertTrue(manager.getTasks().isEmpty());
        Assert.assertEquals(2, factory.opens.size());
        Assert.assertTrue(factory.closes.contains("10.0.0.1:6380"));
        Assert.assertTrue(factory.closes.contains("10.0.0.2:6380"));
        Assert.assertEquals(Collections.singletonList(MINE), factory.releases);
        Assert.assertTrue(monitor.events.contains(
                ShardCompareTaskManager.MONITOR_TYPE + "/" + ShardCompareTaskManager.EVENT_INSUFFICIENT_LANES));
    }

    @Test
    public void testTfsKeeperDropToOneRemovesTask() throws Exception {
        StubMeta meta = twoTfsMine();
        FakeFactory factory = new FakeFactory();
        ShardCompareTaskManager manager = manager(meta, allReady(), factory, allMineAssigner());
        manager.refresh();
        Assert.assertNotNull(manager.taskOf(MINE));

        meta.dc = new DcMeta("jq");
        ClusterMeta cluster = new ClusterMeta("c1");
        addShard(meta.dc, cluster, MINE, "two-tfs",
                keeper("tfs", "10.0.0.1", 6380),
                keeper("DEFAULT", "10.0.0.2", 6380));
        meta.dc.addCluster(cluster);
        manager.refresh();

        Assert.assertNull(manager.taskOf(MINE));
        Assert.assertTrue(manager.getTasks().isEmpty());
        Assert.assertTrue(factory.closes.contains("10.0.0.1:6380"));
        Assert.assertTrue(factory.closes.contains("10.0.0.2:6380"));
        Assert.assertEquals(Collections.singletonList(MINE), factory.releases);
    }

    @Test
    public void testStopRejectsInFlightApply() throws Exception {
        StubMeta meta = twoTfsMine();
        FakeWatchCache cache = allReady();
        FakeFactory factory = new FakeFactory();
        ShardCompareTaskManager manager = manager(meta, cache, factory, allMineAssigner());
        manager.refresh();
        Assert.assertEquals(1, manager.getTasks().size());

        cache.hold = true;
        manager.refresh();
        Assert.assertEquals(2, cache.held.size());
        manager.stop();
        Assert.assertTrue(manager.getTasks().isEmpty());
        Assert.assertEquals(Collections.singletonList(MINE), factory.releases);
        int opensAtStop = factory.opens.size();

        cache.completeHeld(true);
        Assert.assertTrue(manager.getTasks().isEmpty());
        Assert.assertEquals(opensAtStop, factory.opens.size());
    }

    @Test
    public void testRefreshAfterStopDoesNotOpen() throws Exception {
        StubMeta meta = twoTfsMine();
        FakeFactory factory = new FakeFactory();
        ShardCompareTaskManager manager = manager(meta, allReady(), factory, allMineAssigner());
        manager.refresh();
        int opens = factory.opens.size();
        manager.stop();
        Assert.assertTrue(manager.getTasks().isEmpty());

        manager.refresh();
        Assert.assertTrue(manager.getTasks().isEmpty());
        Assert.assertEquals(opens, factory.opens.size());
    }

    @Test
    public void testRefreshThrowableDoesNotStopNextRound() {
        StubMeta meta = twoTfsMine();
        meta.fail = new RuntimeException("boom");
        FakeFactory factory = new FakeFactory();
        CaptureFixedDelaySchedule capture = new CaptureFixedDelaySchedule();
        ShardCompareTaskManager manager = new ShardCompareTaskManager(meta, allMineAssigner(),
                allReady(), factory, new ComparatorConfig(), capture, new RecordingMonitor());
        try {
            manager.start();
            Assert.assertNotNull(capture.task.get());
            capture.task.get().run();
            Assert.assertTrue(manager.getTasks().isEmpty());
            Assert.assertEquals(0, factory.opens.size());

            capture.task.get().run();
            Assert.assertEquals(1, manager.getTasks().size());
            Assert.assertEquals(2, factory.opens.size());
        } finally {
            manager.stop();
            capture.shutdown();
        }
    }

    @Test
    public void testSourceUsesScheduledExecutorAndNoCompareThread() throws Exception {
        String text = new String(Files.readAllBytes(Paths.get(SOURCE)), StandardCharsets.UTF_8);
        Assert.assertTrue(text.contains("SCHEDULED_EXECUTOR"));
        Assert.assertTrue(text.contains("scheduleWithFixedDelay"));
        Assert.assertTrue(text.contains("catch (Throwable"));
        Assert.assertFalse(text.contains("TfsKeeperUtils"));
        Assert.assertFalse(text.contains("new Thread("));
        Assert.assertFalse(text.contains("ShardComparator"));
        Assert.assertFalse(text.contains("new RestTemplate("));
    }

    private ShardCompareTaskManager manager(StubMeta meta, FakeWatchCache cache, FakeFactory factory,
                                            CompareTaskAssigner assigner) {
        return manager(meta, cache, factory, assigner, new RecordingMonitor());
    }

    private ShardCompareTaskManager manager(StubMeta meta, FakeWatchCache cache, FakeFactory factory,
                                            CompareTaskAssigner assigner, EventMonitor monitor) {
        return new ShardCompareTaskManager(meta, assigner, cache, factory, new ComparatorConfig(),
                new CaptureFixedDelaySchedule(), monitor);
    }

    private CompareTaskAssigner allMineAssigner() {
        CaptureFixedDelaySchedule cap = new CaptureFixedDelaySchedule();
        CompareTaskAssigner assigner = new CompareTaskAssigner(new FakeServerGroupProvider(),
                new ComparatorConfig(), new StubFoundation("me"), cap, new RecordingMonitor());
        assigner.start();
        cap.task.get().run();
        return assigner;
    }

    private static FakeWatchCache allReady() {
        FakeWatchCache cache = new FakeWatchCache();
        cache.defaultReady = true;
        return cache;
    }

    private static StubMeta twoTfsMine() {
        StubMeta meta = new StubMeta();
        ClusterMeta cluster = new ClusterMeta("c1");
        addShard(meta.dc, cluster, MINE, "two-tfs",
                keeper("tfs", "10.0.0.1", 6380),
                keeper("tfs", "10.0.0.2", 6380));
        meta.dc.addCluster(cluster);
        return meta;
    }

    private static void addShard(DcMeta dc, ClusterMeta cluster, long dbId, String name, KeeperSpec... keepers) {
        ShardMeta shard = new ShardMeta(name).setDbId(dbId);
        for (KeeperSpec spec : keepers) {
            dc.addKeeperContainer(new KeeperContainerMeta()
                    .setId(spec.containerId)
                    .setDiskType(spec.diskType)
                    .setIp(spec.ip));
            shard.addKeeper(new KeeperMeta()
                    .setKeeperContainerId(spec.containerId)
                    .setIp(spec.ip)
                    .setPort(spec.port));
        }
        cluster.addShard(shard);
    }

    private static KeeperSpec keeper(String diskType, String ip, int port) {
        return new KeeperSpec(diskType, ip, port);
    }

    static final class StubMeta extends ComparatorMetaService {

        DcMeta dc = new DcMeta("jq");

        RuntimeException fail;

        StubMeta() {
            super(new ComparatorConfig());
        }

        @Override
        public DcMeta getCurrentDcMeta() {
            if (fail != null) {
                RuntimeException e = fail;
                fail = null;
                throw e;
            }
            return dc;
        }
    }

    static final class KeeperSpec {

        private static long nextId = 1L;

        final long containerId = nextId++;

        final String diskType;

        final String ip;

        final int port;

        KeeperSpec(String diskType, String ip, int port) {
            this.diskType = diskType;
            this.ip = ip;
            this.port = port;
        }
    }

    static final class FakeWatchCache implements PrepareWatchCache {

        final Map<String, Boolean> answers = new HashMap<>();

        final Map<String, DefaultCommandFuture<Boolean>> held = new LinkedHashMap<>();

        boolean defaultReady;

        boolean hold;

        @Override
        public void invalidateAll() {
        }

        @Override
        public CommandFuture<Boolean> query(Endpoint endpoint) {
            DefaultCommandFuture<Boolean> done = new DefaultCommandFuture<>();
            String key = endpoint.getHost() + ":" + endpoint.getPort();
            if (hold) {
                held.put(key, done);
                return done;
            }
            Boolean hit = answers.get(key);
            if (hit == null && defaultReady) {
                hit = true;
            }
            if (hit == null) {
                done.setFailure(new IllegalStateException("no answer for " + key));
            } else {
                done.setSuccess(hit);
            }
            return done;
        }

        void put(String address, boolean ready) {
            answers.put(address, ready);
        }

        void completeHeld(boolean ok) {
            List<DefaultCommandFuture<Boolean>> snapshot = new ArrayList<>(held.values());
            held.clear();
            for (DefaultCommandFuture<Boolean> future : snapshot) {
                future.setSuccess(ok);
            }
        }
    }

    static final class FakeFactory implements KeeperStreamFactory {

        final List<String> opens = new ArrayList<>();

        final List<String> closes = new ArrayList<>();

        final List<Long> releases = new ArrayList<>();

        @Override
        public CompareLane open(long shardDbId, String cluster, String shard, Endpoint endpoint,
                                Runnable dataAvailable) {
            String key = endpoint.getHost() + ":" + endpoint.getPort();
            opens.add(key);
            return new FakeLane(key);
        }

        @Override
        public void close(CompareLane lane) {
            closes.add(lane.getAddress());
            lane.disconnect();
        }

        @Override
        public void release(long shardDbId) {
            releases.add(shardDbId);
        }
    }

    static final class FakeLane implements CompareLane {

        private final String address;

        private volatile boolean disconnected;

        FakeLane(String address) {
            this.address = address;
        }

        @Override
        public String getAddress() {
            return address;
        }

        @Override
        public String getReplId() {
            return disconnected ? null : "repl";
        }

        @Override
        public long getContinueOffset() {
            return 0;
        }

        @Override
        public StreamRingBuffer getBuffer() {
            return null;
        }

        @Override
        public void disconnect() {
            disconnected = true;
        }

        @Override
        public void reconnect() {
            disconnected = false;
        }
    }

    static final class CmsConfig extends ComparatorConfig {

        @Override
        public String getCmsAccessToken() {
            return "token";
        }

        @Override
        public String getCmsGetServerUrl() {
            return "http://cms.example/GetServer";
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
        public void execute(Runnable command) {
            command.run();
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
                                                         TimeUnit unit) {
            task.set(command);
            return super.schedule(() -> { }, 1, TimeUnit.HOURS);
        }
    }

    static final class RecordingMonitor implements EventMonitor {

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
