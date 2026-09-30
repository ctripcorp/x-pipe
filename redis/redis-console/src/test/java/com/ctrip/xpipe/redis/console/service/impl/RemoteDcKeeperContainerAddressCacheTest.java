package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.core.keeper.container.CurrentDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressSnapshot;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressSnapshot.TombstoneEntry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.ctrip.xpipe.redis.console.service.impl.RemoteDcKeeperContainerAddressCache.ALERT_KEEPER_CONTAINER_IP_OVERLAP;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * keeper-dynamic-ip spec §8.4：远端机房快照的持有、刷新与 IP 重叠告警。
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class RemoteDcKeeperContainerAddressCacheTest extends AbstractConsoleTest {

    private static final String CURRENT_DC = "JQ";
    private static final String DC_OY = "OY";
    private static final String DC_RB = "RB";

    private static final long FETCH_TIMEOUT_MILLI = 300;

    @Mock
    private ConsoleConfig consoleConfig;

    @Mock
    private ConsoleServiceManager consoleServiceManager;

    @Mock
    private CurrentDcKeeperContainerAddressService currentDcAddressService;

    @Mock
    private EventMonitor eventMonitor;

    private ExecutorService fetchExecutor;

    /** 冻结的 tombstone 时钟，单位 nanos。 */
    private final AtomicLong clock = new AtomicLong(TimeUnit.HOURS.toNanos(1));

    /** 阻塞型拉取在用例结束时统一放行，避免线程残留。 */
    private final CountDownLatch release = new CountDownLatch(1);

    private RemoteDcKeeperContainerAddressCache cache;

    @Before
    public void setUp() {
        fetchExecutor = Executors.newCachedThreadPool();
        domains(CURRENT_DC, DC_OY, DC_RB);
        when(consoleConfig.getKeeperContainerAddressRefreshIntervalMilli()).thenReturn(TimeUnit.HOURS.toMillis(1));
        when(currentDcAddressService.getAddressSnapshot()).thenReturn(new KeeperContainerAddressSnapshot());
        when(consoleServiceManager.getKeeperContainerAddressSnapshot(anyString()))
                .thenReturn(new KeeperContainerAddressSnapshot());

        cache = new RemoteDcKeeperContainerAddressCache(consoleConfig, consoleServiceManager, currentDcAddressService,
                fetchExecutor, eventMonitor, CURRENT_DC.toLowerCase(), FETCH_TIMEOUT_MILLI) {
            @Override
            protected long nowNanos() {
                return clock.get();
            }
        };
    }

    @After
    public void tearDown() {
        release.countDown();
        cache.stop();
        fetchExecutor.shutdownNow();
    }

    // ---------------- Jb1：快照持有（T-J.6） ----------------

    @Test
    public void testRefreshReplacesSnapshotAndSkipsCurrentDc() {
        remote(DC_OY, snapshot(map("h1", "10.0.0.1"), Collections.emptyMap(), map("10.0.0.1", "h1")));
        cache.refresh();
        Assert.assertEquals("10.0.0.1", cache.getIpByHostname(DC_OY, "h1"));
        Assert.assertEquals("h1", cache.getHostnameByIp(DC_OY, "10.0.0.1"));

        remote(DC_OY, snapshot(map("h2", "10.0.0.2"), Collections.emptyMap(), map("10.0.0.2", "h2")));
        cache.refresh();
        // 整体替换：旧条目不残留
        Assert.assertNull(cache.getIpByHostname(DC_OY, "h1"));
        Assert.assertNull(cache.getHostnameByIp(DC_OY, "10.0.0.1"));
        Assert.assertEquals("10.0.0.2", cache.getIpByHostname("oy", "h2"));

        Assert.assertEquals(Arrays.asList(DC_OY, DC_RB), cache.getCachedDcs());
        verify(consoleServiceManager, never()).getKeeperContainerAddressSnapshot(CURRENT_DC);
    }

    @Test
    public void testFetchFailureKeepsPreviousSnapshot() {
        remote(DC_OY, snapshot(map("h1", "10.0.0.1"), Collections.emptyMap(), map("10.0.0.1", "h1")));
        cache.refresh();

        when(consoleServiceManager.getKeeperContainerAddressSnapshot(DC_OY))
                .thenThrow(new XpipeRuntimeException("mock http fail"));
        cache.refresh();

        Assert.assertEquals("10.0.0.1", cache.getIpByHostname(DC_OY, "h1"));
        Assert.assertEquals("h1", cache.getHostnameByIp(DC_OY, "10.0.0.1"));
        verify(eventMonitor).logEvent(RemoteDcKeeperContainerAddressCache.CAT_TYPE,
                RemoteDcKeeperContainerAddressCache.CAT_NAME_FETCH_FAIL_PREFIX + DC_OY);
    }

    @Test
    public void testFirstFetchFailureLeavesDcUncached() {
        when(consoleServiceManager.getKeeperContainerAddressSnapshot(DC_OY))
                .thenThrow(new XpipeRuntimeException("mock http fail"));
        cache.refresh();

        Assert.assertEquals(Collections.singletonList(DC_RB), cache.getCachedDcs());
        Assert.assertNull(cache.getIpByHostname(DC_OY, "h1"));
        Assert.assertTrue(cache.getAllHostnameToIp(DC_OY).isEmpty());
    }

    @Test
    public void testRemovedDcDropped() {
        remote(DC_RB, snapshot(map("h1", "10.0.0.1"), Collections.emptyMap(), map("10.0.0.1", "h1")));
        cache.refresh();
        Assert.assertEquals("10.0.0.1", cache.getIpByHostname(DC_RB, "h1"));

        domains(CURRENT_DC, DC_OY);
        cache.refresh();

        Assert.assertEquals(Collections.singletonList(DC_OY), cache.getCachedDcs());
        Assert.assertNull(cache.getIpByHostname(DC_RB, "h1"));
        Assert.assertNull(cache.getHostnameByIp(DC_RB, "10.0.0.1"));
    }

    @Test
    public void testTombstoneExpiresWhileFetchKeepsFailing() {
        remote(DC_OY, snapshot(Collections.emptyMap(), tombstone("h1", "10.0.0.1", 1000), Collections.emptyMap()));
        cache.refresh();

        when(consoleServiceManager.getKeeperContainerAddressSnapshot(DC_OY))
                .thenThrow(new XpipeRuntimeException("mock http fail"));

        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(999));
        cache.refresh();
        Assert.assertEquals("10.0.0.1", cache.getIpByHostname(DC_OY, "h1"));
        Assert.assertEquals(map("h1", "10.0.0.1"), cache.getAllHostnameToIp(DC_OY));

        // 拉取失败保留旧快照，但 tombstone 按接收时的剩余保留期到期，不被延长
        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(1));
        cache.refresh();
        Assert.assertNull(cache.getIpByHostname(DC_OY, "h1"));
        Assert.assertTrue(cache.getAllHostnameToIp(DC_OY).isEmpty());
    }

    @Test
    public void testLookupRulesMatchLocal() {
        Map<String, String> reverse = new HashMap<>();
        reverse.put("10.0.0.1", "h1");
        // 次节点 pod 的 IP 也能反查到 hostname
        reverse.put("10.0.0.3", "h1");
        remote(DC_OY, snapshot(map("h1", "10.0.0.1"), tombstone("h2", "10.0.0.2", 60_000), reverse));
        cache.refresh();

        // 正向：存活优先，未命中查 tombstone
        Assert.assertEquals("10.0.0.1", cache.getIpByHostname(DC_OY, "h1"));
        Assert.assertEquals("10.0.0.2", cache.getIpByHostname(DC_OY, "h2"));
        Map<String, String> all = new HashMap<>();
        all.put("h1", "10.0.0.1");
        all.put("h2", "10.0.0.2");
        Assert.assertEquals(all, cache.getAllHostnameToIp(DC_OY));

        // 反向：只认存活，不回落 tombstone
        Assert.assertEquals("h1", cache.getHostnameByIp(DC_OY, "10.0.0.3"));
        Assert.assertNull(cache.getHostnameByIp(DC_OY, "10.0.0.2"));

        // 查询不跨机房
        Assert.assertNull(cache.getIpByHostname(DC_RB, "h1"));
        Assert.assertNull(cache.getIpByHostname(CURRENT_DC, "h1"));
    }

    // ---------------- Jb2：刷新任务与 IP 重叠告警（T-J.18） ----------------

    @Test
    public void testStartTriggersFirstRoundImmediately() throws Exception {
        CountDownLatch fetched = new CountDownLatch(1);
        when(consoleServiceManager.getKeeperContainerAddressSnapshot(DC_OY)).thenAnswer(inv -> {
            fetched.countDown();
            return snapshot(map("h1", "10.0.0.1"), Collections.emptyMap(), map("10.0.0.1", "h1"));
        });
        domains(CURRENT_DC, DC_OY);

        // 刷新间隔 1h（setUp），能拉到说明是启动即首轮
        cache.start();
        Assert.assertTrue(fetched.await(5, TimeUnit.SECONDS));
        waitConditionUntilTimeOut(() -> "10.0.0.1".equals(cache.getIpByHostname(DC_OY, "h1")), 5000);
    }

    @Test
    public void testRoundsNotOverlap() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        AtomicInteger fetchCount = new AtomicInteger();
        when(consoleServiceManager.getKeeperContainerAddressSnapshot(DC_OY)).thenAnswer(inv -> {
            // 只卡住首轮，之后的轮次立即返回
            if (fetchCount.incrementAndGet() == 1) {
                entered.countDown();
                release.await();
            }
            return snapshot(map("h1", "10.0.0.1"), Collections.emptyMap(), map("10.0.0.1", "h1"));
        });
        domains(CURRENT_DC, DC_OY);
        long intervalMilli = 50;
        when(consoleConfig.getKeeperContainerAddressRefreshIntervalMilli()).thenReturn(intervalMilli);
        // 超时放宽，让首轮卡在拉取里
        cache = new RemoteDcKeeperContainerAddressCache(consoleConfig, consoleServiceManager, currentDcAddressService,
                fetchExecutor, eventMonitor, CURRENT_DC, TimeUnit.MINUTES.toMillis(1));

        cache.start();
        Assert.assertTrue(entered.await(5, TimeUnit.SECONDS));

        // 首轮卡住期间过去了多个刷新间隔，也不会起第二轮
        sleep((int) (intervalMilli * 10));
        Assert.assertEquals(1, fetchCount.get());

        // 首轮结束后按间隔继续
        release.countDown();
        waitConditionUntilTimeOut(() -> fetchCount.get() >= 3, 5000);
        Assert.assertEquals("10.0.0.1", cache.getIpByHostname(DC_OY, "h1"));
    }

    @Test
    public void testSingleDcTimeoutDoesNotAffectOthers() {
        remote(DC_OY, snapshot(map("h1", "10.0.0.1"), Collections.emptyMap(), map("10.0.0.1", "h1")));
        remote(DC_RB, snapshot(map("h2", "10.0.0.2"), Collections.emptyMap(), map("10.0.0.2", "h2")));
        cache.refresh();

        when(consoleServiceManager.getKeeperContainerAddressSnapshot(DC_OY)).thenAnswer(inv -> {
            release.await();
            return snapshot(map("h1", "10.9.9.9"), Collections.emptyMap(), Collections.emptyMap());
        });
        remote(DC_RB, snapshot(map("h3", "10.0.0.3"), Collections.emptyMap(), map("10.0.0.3", "h3")));

        long begin = System.nanoTime();
        cache.refresh();
        long costMilli = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

        // 超时机房保留旧快照，其余机房照常替换，整轮不超过超时太多
        Assert.assertEquals("10.0.0.1", cache.getIpByHostname(DC_OY, "h1"));
        Assert.assertEquals("10.0.0.3", cache.getIpByHostname(DC_RB, "h3"));
        Assert.assertNull(cache.getIpByHostname(DC_RB, "h2"));
        Assert.assertTrue("cost " + costMilli, costMilli < FETCH_TIMEOUT_MILLI + 1000);
        verify(eventMonitor).logEvent(RemoteDcKeeperContainerAddressCache.CAT_TYPE,
                RemoteDcKeeperContainerAddressCache.CAT_NAME_FETCH_FAIL_PREFIX + DC_OY);

        // 迟到的结果不会写进缓存
        release.countDown();
        sleep(200);
        Assert.assertEquals("10.0.0.1", cache.getIpByHostname(DC_OY, "h1"));
    }

    @Test
    public void testIpOverlapAlertsOncePerRoundWithEventNameOnly() {
        when(currentDcAddressService.getAddressSnapshot())
                .thenReturn(snapshot(map("hA", "10.0.0.1"), Collections.emptyMap(), map("10.0.0.1", "hA")));
        remote(DC_OY, snapshot(map("hB", "10.0.0.1"), Collections.emptyMap(), map("10.0.0.1", "hB", "10.0.0.2", "hC")));
        remote(DC_RB, snapshot(map("hD", "10.0.0.2"), Collections.emptyMap(), map("10.0.0.2", "hD")));

        cache.refresh();
        verify(eventMonitor, times(1)).logAlertEvent(anyString());
        verify(eventMonitor, times(1)).logAlertEvent(ALERT_KEEPER_CONTAINER_IP_OVERLAP);

        // 条件持续则每轮都报
        cache.refresh();
        verify(eventMonitor, times(2)).logAlertEvent(anyString());
        verify(eventMonitor, times(2)).logAlertEvent(ALERT_KEEPER_CONTAINER_IP_OVERLAP);
    }

    @Test
    public void testNoOverlapNoAlert() {
        when(currentDcAddressService.getAddressSnapshot())
                .thenReturn(snapshot(map("hA", "10.0.0.1"), Collections.emptyMap(), map("10.0.0.1", "hA")));
        // tombstone 的 IP 与本机房相同不算重叠：检测只看存活反向
        remote(DC_OY, snapshot(map("hB", "10.0.0.2"), tombstone("hC", "10.0.0.1", 60_000), map("10.0.0.2", "hB")));
        remote(DC_RB, snapshot(map("hD", "10.0.0.3"), Collections.emptyMap(), map("10.0.0.3", "hD")));

        cache.refresh();
        cache.refresh();
        verify(eventMonitor, never()).logAlertEvent(anyString());
    }

    // ---------------- helpers ----------------

    private void domains(String... dcs) {
        Map<String, String> domains = new HashMap<>();
        for (String dc : dcs) {
            // 大小写混用，验证机房名统一按大写处理
            domains.put(dc.toLowerCase(), "http://" + dc.toLowerCase() + ".console");
        }
        when(consoleConfig.getConsoleDomains()).thenReturn(domains);
    }

    private void remote(String dc, KeeperContainerAddressSnapshot snapshot) {
        doReturn(snapshot).when(consoleServiceManager).getKeeperContainerAddressSnapshot(dc);
    }

    private KeeperContainerAddressSnapshot snapshot(Map<String, String> live, Map<String, TombstoneEntry> tombstones,
                                                    Map<String, String> reverse) {
        return new KeeperContainerAddressSnapshot(new HashMap<>(live), new HashMap<>(tombstones), new HashMap<>(reverse));
    }

    private Map<String, TombstoneEntry> tombstone(String hostname, String ip, long remainingMillis) {
        return Collections.singletonMap(hostname, new TombstoneEntry(ip, remainingMillis));
    }

    private Map<String, String> map(String... kvs) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put(kvs[i], kvs[i + 1]);
        }
        return map;
    }
}
