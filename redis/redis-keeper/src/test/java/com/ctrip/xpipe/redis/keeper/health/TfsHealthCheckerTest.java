package com.ctrip.xpipe.redis.keeper.health;

import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.HealthState;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperContainerConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperContainerConfig;
import com.ctrip.xpipe.redis.keeper.storage.EIOException;
import com.ctrip.xpipe.redis.keeper.storage.health.IoTask;
import com.ctrip.xpipe.redis.keeper.storage.health.IoTelemetry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TfsHealthCheckerTest {

    private MetricProxy metricProxy;
    private TestKeeperContainerConfig config;
    private IoTelemetry telemetry;
    private TfsHealthChecker checker;
    private RedisKeeperServer keeper1;
    private RedisKeeperServer keeper2;
    private List<RedisKeeperServer> keepers;
    private List<Object> notified;
    private long now;

    @Before
    public void setUp() {
        metricProxy = mock(MetricProxy.class);
        config = new TestKeeperContainerConfig("/tmp/keeper");
        config.setMode(KeeperContainerConfig.MODE_TFS);
        telemetry = new IoTelemetry();
        // past the warm-up: the rule tests are about the rules, not the ramp-up
        telemetry.startAt(System.nanoTime() - TimeUnit.MINUTES.toNanos(1));

        keeper1 = keeper(1L);
        keeper2 = keeper(2L);
        keepers = new ArrayList<>(Arrays.asList(keeper1, keeper2));

        notified = new ArrayList<>();
        newChecker();
    }

    private void newChecker() {
        checker = new TfsHealthChecker();
        ReflectionTestUtils.setField(checker, "config", config);
        ReflectionTestUtils.setField(checker, "mode", "TFS");
        ReflectionTestUtils.setField(checker, "az", "az1");
        ReflectionTestUtils.setField(checker, "group", "group1");
        checker.bindKeepers(() -> keepers);
        checker.telemetry = telemetry;
        checker.metricProxy = metricProxy;
        notified.clear();
        checker.addObserver((args, observable) -> notified.add(args));
    }

    @Test
    public void testContainerFailureRatioMarksEveryKeeper() {
        // 1000+ total so the 30s window clears the 1000-record floor ("30s没到1000就不判断");
        // 20/1000 = 2% > 1%: container dimension, so the healthy shard is marked too
        for (int i = 0; i < 980; i++) {
            record(null, "/data/repl", false);
        }
        for (int i = 0; i < 20; i++) {
            record("repl_2", "/data/repl", true);
        }

        checker.check(System.nanoTime());

        Assert.assertEquals(HealthState.SICK, checker.state("repl_1"));
        Assert.assertEquals(HealthState.SICK, checker.state("repl_2"));
        // SICK is metric-only: nothing notified on the first anomalous tick
        Assert.assertTrue(notified.isEmpty());
    }

    @Test
    public void testContainerRatioRulesAreSkippedBelowTheRecordFloor() {
        // same 2% fail ratio as above, but only 100 total in the 30s window: too small a sample
        // to trust a percentage against, so R1 must not fire at all ("30s没到1000就不判断")
        for (int i = 0; i < 98; i++) {
            record(null, "/data/repl", false);
        }
        for (int i = 0; i < 2; i++) {
            record("repl_2", "/data/repl", true);
        }

        checker.check(System.nanoTime());

        Assert.assertEquals(HealthState.HEALTHY, checker.state("repl_1"));
        Assert.assertEquals(HealthState.HEALTHY, checker.state("repl_2"));
    }

    @Test
    public void testSecondAnomalousTickNotifiesDown() {
        for (int i = 0; i < 980; i++) {
            record(null, "/data/repl", false);
        }
        for (int i = 0; i < 20; i++) {
            record("repl_2", "/data/repl", true);
        }

        checker.check(System.nanoTime());
        checker.check(System.nanoTime());

        Assert.assertEquals(2, notified.size());
        // the payload is the replId; the health state is written onto the keeper's meta
        for (Object n : notified) {
            String tenant = (String) n;
            Assert.assertTrue(tenant.equals("repl_1") || tenant.equals("repl_2"));
            Assert.assertEquals(HealthState.DOWN, checker.state(tenant));
        }
    }

    @Test
    public void testTenantRuleOnlyMarksTheBadShard() {
        // 6 failures over 606 tasks keeps the container ratio under 1%
        for (int i = 0; i < 600; i++) {
            record(null, "/data/repl", false);
        }
        for (int i = 0; i < 6; i++) {
            record("repl_1", "/data/repl", true);
        }

        checker.check(System.nanoTime());

        Assert.assertEquals(HealthState.SICK, checker.state("repl_1"));
        Assert.assertEquals(HealthState.HEALTHY, checker.state("repl_2"));
    }

    @Test
    public void testSingleFileConsecutiveFailuresMarkItsTenant() {
        for (int i = 0; i < 600; i++) {
            record(null, "/data/repl", false);
        }
        record("repl_1", "/data/bad", true);
        record("repl_1", "/data/bad", true);
        record("repl_1", "/data/bad", true);

        checker.check(System.nanoTime());

        Assert.assertEquals(HealthState.SICK, checker.state("repl_1"));
        Assert.assertEquals(HealthState.HEALTHY, checker.state("repl_2"));
    }

    @Test
    public void testSuccessResetsTheFileConsecutiveFails() {
        for (int i = 0; i < 600; i++) {
            record(null, "/data/repl", false);
        }
        record("repl_1", "/data/bad", true);
        record("repl_1", "/data/bad", true);
        record("repl_1", "/data/bad", false);
        record("repl_1", "/data/bad", true);

        checker.check(System.nanoTime());

        // 3 fails in 10s but never 3 in a row, and under the tenant floor
        Assert.assertEquals(HealthState.HEALTHY, checker.state("repl_1"));
    }

    @Test
    public void testWarmUpNeedsBothRecordsAndElapsedTime() {
        telemetry = new IoTelemetry();
        checker.telemetry = telemetry;
        for (int i = 0; i < 100; i++) {
            record("repl_1", "/data/repl", true);
        }

        checker.check(System.nanoTime());
        Assert.assertEquals(HealthState.HEALTHY, checker.state("repl_1"));

        telemetry.startAt(System.nanoTime() - TimeUnit.MINUTES.toNanos(1));
        checker.check(System.nanoTime());
        Assert.assertEquals(HealthState.SICK, checker.state("repl_1"));
    }

    @Test
    public void testKeeperRemovedFromContainerDropsItsState() {
        for (int i = 0; i < 600; i++) {
            record(null, "/data/repl", false);
        }
        for (int i = 0; i < 6; i++) {
            record("repl_1", "/data/repl", true);
        }
        checker.check(System.nanoTime());
        Assert.assertEquals(HealthState.SICK, checker.state("repl_1"));

        keepers.remove(keeper1);
        record(null, "/data/repl", false);
        checker.check(System.nanoTime());

        Assert.assertEquals(HealthState.HEALTHY, checker.state("repl_1"));
    }

    @Test
    public void testFreshStartSeedsFromZkInsteadOfAssumingHealthy() {
        // zk already holds DOWN for this keeper when the checker (re)starts: must resume there,
        // not silently assume HEALTHY and leave the latch node stuck forever
        when(keeper1.getCurrentKeeperMeta())
                .thenReturn(new KeeperMeta().setHealthState(HealthState.DOWN.name()));
        for (int i = 0; i < 600; i++) {
            record(null, "/data/repl", false);
        }

        checker.check(System.nanoTime());

        // no anomaly this tick: DOWN -> RECOVERED_PENDING, not silently HEALTHY
        Assert.assertEquals(HealthState.RECOVERED_PENDING, checker.state("repl_1"));
        Assert.assertEquals(1, notified.size());
        Assert.assertEquals("repl_1", notified.get(0));
        Assert.assertEquals(HealthState.RECOVERED_PENDING, checker.state("repl_1"));
    }

    @Test
    public void testReRegisteredKeeperReseedsFromZkRatherThanHealthy() {
        for (int i = 0; i < 600; i++) {
            record(null, "/data/repl", false);
        }
        checker.check(System.nanoTime());
        Assert.assertEquals(HealthState.HEALTHY, checker.state("repl_1"));

        keepers.remove(keeper1);
        checker.check(System.nanoTime());

        // keeper1 re-registers; zk still carries RECOVERED_PENDING from an earlier incident the
        // operator has not cleared through the API yet
        when(keeper1.getCurrentKeeperMeta())
                .thenReturn(new KeeperMeta().setHealthState(HealthState.RECOVERED_PENDING.name()));
        keepers.add(keeper1);

        checker.check(System.nanoTime());
        Assert.assertEquals(HealthState.RECOVERED_PENDING, checker.state("repl_1"));
    }

    @Test
    public void testDownIsRepublishedEachTickUntilElectionResetConfirms() {
        // only repl_1 in the container, driven to DOWN purely by the tenant floor (R4): pad with
        // null-tenant successes so the container ratio stays quiet and this stays a per-tenant case.
        keepers.remove(keeper2);
        when(keeper1.getCurrentKeeperMeta()).thenReturn(new KeeperMeta());

        driveTenantAnomaly();
        checker.check(System.nanoTime());            // HEALTHY -> SICK (metric only)
        Assert.assertEquals(HealthState.SICK, checker.state("repl_1"));
        Assert.assertTrue(notified.isEmpty());

        driveTenantAnomaly();
        checker.check(System.nanoTime());            // SICK -> DOWN, first publish
        Assert.assertEquals(HealthState.DOWN, checker.state("repl_1"));
        Assert.assertEquals(1, notified.size());
        Assert.assertEquals("repl_1", notified.get(0));

        // reset was throttled / failed: nobody called onElectionReset, so the next tick republishes
        driveTenantAnomaly();
        checker.check(System.nanoTime());
        Assert.assertEquals(2, notified.size());

        // container confirms the reset landed in zk; the stable DOWN must stop re-notifying
        checker.onElectionReset("repl_1");
        driveTenantAnomaly();
        checker.check(System.nanoTime());
        Assert.assertEquals(2, notified.size());
    }

    /** Enough window volume to pass the record floor, with repl_1 over the tenant abnormal floor. */
    private void driveTenantAnomaly() {
        for (int i = 0; i < 200; i++) {
            record(null, "/data/other", false);
        }
        for (int i = 0; i < 10; i++) {
            record("repl_1", "/data/repl", true);
        }
    }

    @Test
    public void testRecoveredPendingStaysUntilTheApiClearsIt() {
        Assert.assertEquals(HealthState.HEALTHY, TfsHealthChecker.next(HealthState.HEALTHY, false));
        Assert.assertEquals(HealthState.SICK, TfsHealthChecker.next(HealthState.HEALTHY, true));
        Assert.assertEquals(HealthState.HEALTHY, TfsHealthChecker.next(HealthState.SICK, false));
        Assert.assertEquals(HealthState.DOWN, TfsHealthChecker.next(HealthState.SICK, true));
        Assert.assertEquals(HealthState.RECOVERED_PENDING, TfsHealthChecker.next(HealthState.DOWN, false));
        Assert.assertEquals(HealthState.DOWN, TfsHealthChecker.next(HealthState.DOWN, true));
        Assert.assertEquals(HealthState.RECOVERED_PENDING,
                TfsHealthChecker.next(HealthState.RECOVERED_PENDING, true));
        Assert.assertEquals(HealthState.RECOVERED_PENDING,
                TfsHealthChecker.next(HealthState.RECOVERED_PENDING, false));
    }

    @Test
    public void testRecoveredPendingIsNeverReElected() {
        Assert.assertFalse(HealthState.RECOVERED_PENDING.isElectable());
        Assert.assertFalse(HealthState.DOWN.isElectable());
        Assert.assertFalse(HealthState.SICK.isElectable());
        Assert.assertTrue(HealthState.HEALTHY.isElectable());
        Assert.assertTrue(HealthState.isElectable(null));
    }

    @Test
    public void testRulesAreReadFromConfig() {
        config.setIoHealthFailRatioPercent(2);
        newChecker();
        // null tenant so this is purely the container ratio (R1), not also tripping the tenant
        // floor (R4) the way tagging these as repl_1 would
        for (int i = 0; i < 980; i++) {
            record(null, "/data/repl", false);
        }
        for (int i = 0; i < 20; i++) {
            record(null, "/data/repl", true);
        }

        // 20/1000 = 2% is not strictly above 2%
        checker.check(System.nanoTime());
        Assert.assertEquals(HealthState.HEALTHY, checker.state("repl_1"));

        config.setIoHealthFailRatioPercent(1);
        newChecker();
        checker.check(System.nanoTime());
        Assert.assertEquals(HealthState.SICK, checker.state("repl_1"));
    }

    @Test
    public void testMetricIsContainerLevelZeroWhenHealthy() throws Exception {
        record(null, "/data/repl", false);

        checker.reportMetric();

        ArgumentCaptor<MetricData> captor = ArgumentCaptor.forClass(MetricData.class);
        verify(metricProxy, times(1)).writeBinMultiDataPoint(captor.capture());
        MetricData data = captor.getValue();
        Assert.assertEquals(TfsHealthChecker.METRIC_TYPE, data.getMetricType());
        Assert.assertEquals(0.0d, data.getValue(), 0.0d);
        Assert.assertEquals(HealthState.HEALTHY.name(), data.getTags().get("state"));
        Assert.assertEquals("TFS", data.getTags().get("mode"));
        Assert.assertEquals("az1", data.getTags().get("az"));
        Assert.assertEquals("group1", data.getTags().get("group"));
    }

    @Test
    public void testMetricIsOneWhenAnyKeeperIsAbnormal() throws Exception {
        // trip the container rule, then confirm the metric reports 1
        for (int i = 0; i < 980; i++) {
            record(null, "/data/repl", false);
        }
        for (int i = 0; i < 20; i++) {
            record("repl_1", "/data/repl", true);
        }
        checker.check(System.nanoTime());
        Assert.assertEquals(HealthState.SICK, checker.state("repl_1"));

        checker.reportMetric();

        ArgumentCaptor<MetricData> captor = ArgumentCaptor.forClass(MetricData.class);
        verify(metricProxy, times(1)).writeBinMultiDataPoint(captor.capture());
        Assert.assertEquals(1.0d, captor.getValue().getValue(), 0.0d);
        Assert.assertEquals(HealthState.SICK.name(), captor.getValue().getTags().get("state"));
    }

    @Test
    public void testRunningTaskAboveFiveSecondsIsAHang() {
        useFakeClock();
        baseline(600);
        start("repl_1", "/data/stuck");
        now += TimeUnit.SECONDS.toNanos(6);

        checker.check(now);

        // R2 is container-wide: the shard with no IO at all is marked too
        Assert.assertEquals(HealthState.SICK, checker.state("repl_1"));
        Assert.assertEquals(HealthState.SICK, checker.state("repl_2"));
    }

    @Test
    public void testFinishedTaskAboveFiveSecondsIsAHang() {
        useFakeClock();
        baseline(600);
        IoTask<Void> task = start("repl_1", "/data/slow");
        now += TimeUnit.SECONDS.toNanos(6);
        telemetry.taskCompleted(task, null);

        checker.check(now);

        Assert.assertEquals(HealthState.SICK, checker.state("repl_1"));
        Assert.assertEquals(HealthState.SICK, checker.state("repl_2"));
    }

    @Test
    public void testQueueWaitIsNotAHang() {
        useFakeClock();
        baseline(600);
        // both waited 10s in the queue, then ran for 1s: one finished, one still running
        IoTask<Void> finished = new IoTask<>("repl_1", "/data/a", now, () -> null);
        IoTask<Void> running = new IoTask<>("repl_1", "/data/b", now, () -> null);
        now += TimeUnit.SECONDS.toNanos(10);
        telemetry.taskStarted(finished);
        telemetry.taskStarted(running);
        now += TimeUnit.SECONDS.toNanos(1);
        telemetry.taskCompleted(finished, null);

        checker.check(now);

        Assert.assertEquals(HealthState.HEALTHY, checker.state("repl_1"));
        Assert.assertEquals(HealthState.HEALTHY, checker.state("repl_2"));
    }

    @Test
    public void testRunningSlowTasksCountTowardTheSlowRatio() {
        useFakeClock();
        baseline(1000);
        // 53 running for 3s plus 100 running for 1s, all below the 5s hang line. Only tasks whose
        // slowness is known count: 53 / (1000 + 53) ≈ 5.03% ≥ 5%. Counting the undecided 1s ones
        // too would give 53 / 1153 ≈ 4.6% and not fire.
        runSlowAndFast(null, 53, 100);

        checker.check(now);

        Assert.assertEquals(HealthState.SICK, checker.state("repl_1"));
        Assert.assertEquals(HealthState.SICK, checker.state("repl_2"));
    }

    @Test
    public void testRunningSlowTasksAlsoJoinTheSlowRatioDenominator() {
        useFakeClock();
        baseline(1000);
        // 52 / (1000 + 52) ≈ 4.94% < 5%. Leaving them out of the denominator would give
        // 52 / 1000 = 5.2% and fire.
        runSlowAndFast(null, 52, 0);

        checker.check(now);

        Assert.assertEquals(HealthState.HEALTHY, checker.state("repl_1"));
        Assert.assertEquals(HealthState.HEALTHY, checker.state("repl_2"));
    }

    @Test
    public void testRunningSlowTasksCountTowardTheTenantRule() {
        useFakeClock();
        // under the 1000-record floor, so only the tenant rule can fire
        baseline(600);
        for (int i = 0; i < 200; i++) {
            record("repl_1", "/data/repl", false);
        }
        runSlowAndFast("repl_1", 11, 20);

        checker.check(now);

        // 11 > max(5, (200 + 11) * 5%) = 10. Counting the 20 undecided ones too would make the
        // threshold (200 + 11 + 20) * 5% = 11 and not fire.
        Assert.assertEquals(HealthState.SICK, checker.state("repl_1"));
        Assert.assertEquals(HealthState.HEALTHY, checker.state("repl_2"));
    }

    @Test
    public void testRunningSlowTasksAlsoJoinTheTenantDenominator() {
        useFakeClock();
        baseline(600);
        for (int i = 0; i < 209; i++) {
            record("repl_1", "/data/repl", false);
        }
        runSlowAndFast("repl_1", 11, 0);

        checker.check(now);

        // 11 is not > max(5, (209 + 11) * 5%) = 11. Leaving them out of the denominator would
        // make the threshold 209 * 5% = 10 and fire.
        Assert.assertEquals(HealthState.HEALTHY, checker.state("repl_1"));
    }

    /** {@code slow} tasks running for 3s and {@code fast} ones running for 1s at {@code now}. */
    private void runSlowAndFast(String tenant, int slow, int fast) {
        for (int i = 0; i < slow; i++) {
            start(tenant, "/data/slow-" + i);
        }
        now += TimeUnit.SECONDS.toNanos(2);
        for (int i = 0; i < fast; i++) {
            start(tenant, "/data/fast-" + i);
        }
        now += TimeUnit.SECONDS.toNanos(1);
    }

    private void useFakeClock() {
        now = System.nanoTime();
        telemetry.clock = () -> now;
    }

    private void baseline(int count) {
        for (int i = 0; i < count; i++) {
            record(null, "/data/repl", false);
        }
    }

    @Test
    public void testNonTfsContainerIsNotChecked() {
        // a BM container measures its local disk on the same IO pool; that is DiskHealthChecker's
        // job, so this state machine must stay out of it entirely
        config.setMode(KeeperContainerConfig.MODE_NORMAL);
        for (int i = 0; i < 600; i++) {
            record("repl_1", "/data/repl", true);
        }

        checker.check(System.nanoTime());
        checker.check(System.nanoTime());

        Assert.assertEquals(HealthState.HEALTHY, checker.state("repl_1"));
        Assert.assertTrue(notified.isEmpty());
    }

    private IoTask<Void> start(String tenant, String path) {
        IoTask<Void> task = new IoTask<>(tenant, path, now, () -> null);
        telemetry.taskStarted(task);
        return task;
    }

    private RedisKeeperServer keeper(long replId) {
        RedisKeeperServer keeper = mock(RedisKeeperServer.class);
        when(keeper.getReplId()).thenReturn(ReplId.from(replId));
        return keeper;
    }

    private void record(String tenant, String path, boolean failed) {
        IoTask<Void> task = new IoTask<>(tenant, path, System.nanoTime(), () -> null);
        telemetry.taskStarted(task);
        telemetry.taskCompleted(task, failed ? new EIOException(new IOException("Input/output error")) : null);
    }
}
