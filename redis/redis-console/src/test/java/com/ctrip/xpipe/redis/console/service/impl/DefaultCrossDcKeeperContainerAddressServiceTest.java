package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.core.keeper.container.CurrentDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressSnapshot;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressSnapshot.TombstoneEntry;
import com.ctrip.xpipe.tuple.Pair;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * keeper-dynamic-ip spec §8.5：跨机房查询门面只读内存。本机房读 CurrentDc，远端读快照缓存，查询不发 HTTP。
 * <p>
 * 缓存用真实 {@link RemoteDcKeeperContainerAddressCache}（不 start，手动 refresh），HTTP 只经 mock 的
 * {@link ConsoleServiceManager}，据此验证查询路径上没有 IO。
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultCrossDcKeeperContainerAddressServiceTest {

    private static final String CURRENT_DC = "JQ";
    private static final String DC_OY = "OY";
    private static final String DC_RB = "RB";
    private static final String DC_FRA = "FRA";

    @Mock
    private ConsoleConfig consoleConfig;

    @Mock
    private ConsoleServiceManager consoleServiceManager;

    @Mock
    private CurrentDcKeeperContainerAddressService currentDcAddressService;

    @Mock
    private EventMonitor eventMonitor;

    private ExecutorService fetchExecutor;

    private RemoteDcKeeperContainerAddressCache cache;

    private DefaultCrossDcKeeperContainerAddressService service;

    @Before
    public void setUp() {
        fetchExecutor = Executors.newCachedThreadPool();
        Map<String, String> domains = new HashMap<>();
        for (String dc : Arrays.asList(CURRENT_DC, DC_OY, DC_RB, DC_FRA)) {
            domains.put(dc.toLowerCase(), "http://" + dc.toLowerCase() + ".console");
        }
        when(consoleConfig.getConsoleDomains()).thenReturn(domains);
        when(currentDcAddressService.getAddressSnapshot()).thenReturn(new KeeperContainerAddressSnapshot());
        when(consoleServiceManager.getKeeperContainerAddressSnapshot(anyString()))
                .thenReturn(new KeeperContainerAddressSnapshot());

        cache = new RemoteDcKeeperContainerAddressCache(consoleConfig, consoleServiceManager, currentDcAddressService,
                fetchExecutor, eventMonitor, CURRENT_DC, 3000);
        service = new DefaultCrossDcKeeperContainerAddressService(currentDcAddressService, cache,
                CURRENT_DC.toLowerCase());
    }

    @After
    public void tearDown() {
        fetchExecutor.shutdownNow();
    }

    @Test
    public void testCurrentDcReadsLocalOnly() {
        // 远端快照里也有同名 hostname，本机房查询不应读到
        remote(DC_OY, snapshot(map("h1", "10.0.1.1"), Collections.emptyMap(), map("10.0.1.1", "h1")));
        refreshThenForgetHttp();
        when(currentDcAddressService.getIpByHostname("h1")).thenReturn("10.0.0.1");
        when(currentDcAddressService.getAllHostnameToIp()).thenReturn(map("h1", "10.0.0.1"));

        Assert.assertEquals("10.0.0.1", service.getIpByHostname("jq", "h1"));
        Assert.assertEquals(map("h1", "10.0.0.1"), service.getAllHostnameToIp(CURRENT_DC));
        Assert.assertNull(service.getIpByHostname(CURRENT_DC, "absent"));

        verifyNoInteractions(consoleServiceManager);
    }

    @Test
    public void testRemoteReadsSnapshotWithoutHttp() {
        remote(DC_OY, snapshot(map("h1", "10.0.1.1"), tombstone("h2", "10.0.1.2", 60_000),
                map("10.0.1.1", "h1")));
        refreshThenForgetHttp();

        Assert.assertEquals("10.0.1.1", service.getIpByHostname("oy", "h1"));
        // tombstone 与本地语义一致：正向可见
        Assert.assertEquals("10.0.1.2", service.getIpByHostname(DC_OY, "h2"));
        Map<String, String> all = map("h1", "10.0.1.1", "h2", "10.0.1.2");
        Assert.assertEquals(all, service.getAllHostnameToIp(DC_OY));
        // 不跨机房回落
        Assert.assertNull(service.getIpByHostname(DC_RB, "h1"));

        verifyNoInteractions(consoleServiceManager);
        verify(currentDcAddressService, never()).getIpByHostname(anyString());
        verify(currentDcAddressService, never()).getAllHostnameToIp();
    }

    @Test
    public void testUnknownDcLocalFirstThenRemoteByDcName() {
        remote(DC_RB, snapshot(map("h1", "10.0.2.1", "h2", "10.0.2.2"), Collections.emptyMap(),
                map("10.0.2.1", "h1", "10.0.2.2", "h2")));
        remote(DC_OY, snapshot(map("h2", "10.0.1.2"), Collections.emptyMap(), map("10.0.1.2", "h2")));
        refreshThenForgetHttp();

        // 本机房命中直接返回，不看远端
        when(currentDcAddressService.getIpByHostname("h1")).thenReturn("10.0.0.1");
        Assert.assertEquals("10.0.0.1", service.getIpByHostname("h1"));

        // 本机房未命中：多机房命中按机房名字典序取首（OY < RB）
        Assert.assertEquals("10.0.1.2", service.getIpByHostname("h2"));

        Assert.assertNull(service.getIpByHostname("absent"));
        verifyNoInteractions(consoleServiceManager);
    }

    @Test
    public void testFindHostnamesByIpReturnsAllCandidates() {
        remote(DC_RB, snapshot(map("hR", "10.0.0.1"), Collections.emptyMap(), map("10.0.0.1", "hR")));
        remote(DC_OY, snapshot(map("hO", "10.0.0.1"), Collections.emptyMap(), map("10.0.0.1", "hO")));
        // FRA 的 10.0.0.1 只是 tombstone，不参与反查
        remote(DC_FRA, snapshot(Collections.emptyMap(), tombstone("hF", "10.0.0.1", 60_000), Collections.emptyMap()));
        refreshThenForgetHttp();

        // 本机房在前，远端按机房名字典序
        when(currentDcAddressService.getHostnameByIp("10.0.0.1")).thenReturn("hJ");
        Assert.assertEquals(Arrays.asList(Pair.of(CURRENT_DC, "hJ"), Pair.of(DC_OY, "hO"), Pair.of(DC_RB, "hR")),
                service.findHostnamesByIp("10.0.0.1"));

        // 本机房未命中时只有远端候选
        when(currentDcAddressService.getHostnameByIp("10.0.0.1")).thenReturn(null);
        Assert.assertEquals(Arrays.asList(Pair.of(DC_OY, "hO"), Pair.of(DC_RB, "hR")),
                service.findHostnamesByIp("10.0.0.1"));

        Assert.assertTrue(service.findHostnamesByIp("10.9.9.9").isEmpty());
        verifyNoInteractions(consoleServiceManager);
    }

    @Test
    public void testRemoteUnresolvableBeforeFirstRefresh() {
        remote(DC_OY, snapshot(map("h1", "10.0.1.1"), Collections.emptyMap(), map("10.0.1.1", "h1")));
        when(currentDcAddressService.getHostnameByIp("10.0.1.1")).thenReturn("hJ");

        // 未刷新：远端查询全部解析不到，也不会临时发起拉取
        Assert.assertNull(service.getIpByHostname(DC_OY, "h1"));
        Assert.assertTrue(service.getAllHostnameToIp(DC_OY).isEmpty());
        Assert.assertNull(service.getIpByHostname("h1"));
        Assert.assertEquals(Collections.singletonList(Pair.of(CURRENT_DC, "hJ")), service.findHostnamesByIp("10.0.1.1"));
        verifyNoInteractions(consoleServiceManager);

        cache.refresh();
        Assert.assertEquals("10.0.1.1", service.getIpByHostname(DC_OY, "h1"));
        Assert.assertEquals("10.0.1.1", service.getIpByHostname("h1"));
    }

    @Test
    public void testEmptyArguments() {
        Assert.assertNull(service.getIpByHostname(DC_OY, null));
        Assert.assertNull(service.getIpByHostname(null, "h1"));
        Assert.assertNull(service.getIpByHostname(""));
        Assert.assertTrue(service.getAllHostnameToIp("").isEmpty());
        Assert.assertTrue(service.findHostnamesByIp(null).isEmpty());
        verifyNoInteractions(consoleServiceManager);
    }

    // ---------------- helpers ----------------

    /** 刷新一轮后清掉拉取产生的调用记录，之后对 consoleServiceManager 的任何调用都来自查询路径。 */
    private void refreshThenForgetHttp() {
        cache.refresh();
        clearInvocations(consoleServiceManager, currentDcAddressService);
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
