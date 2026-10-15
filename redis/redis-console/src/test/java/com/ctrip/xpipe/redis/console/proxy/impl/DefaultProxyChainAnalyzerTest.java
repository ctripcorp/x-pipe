package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardPeer;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollector;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollectorManager;
import com.ctrip.xpipe.redis.console.service.CrossDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.ctrip.xpipe.redis.console.proxy.impl.DefaultProxyChainAnalyzer.ALERT_PROXY_CHAIN_KEEPER_AMBIGUOUS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * keeper-dynamic-ip spec §8.6：链路 dst 的网络地址翻译为 meta 地址；本机房 proxy 为第一跳时 backup 取 tunnel 所在机房，
 * 否则（peer / relay）src 按网络 IP 归属机房；多命中告警并跳过。
 * <p>
 * 执行器用 direct executor，{@code fullUpdate()} 返回时本轮分析已完成。
 */
public class DefaultProxyChainAnalyzerTest extends AbstractProxyChainTest {

    private static final String CLUSTER = "cluster";
    private static final String SHARD = "shard";
    private static final Pair<String, String> CLUSTER_SHARD = new Pair<>(CLUSTER, SHARD);

    private static final String DC_JQ = "JQ";
    private static final String DC_OY = "OY";
    private static final String DC_RB = "RB";

    private static final int KEEPER_PORT = 6380;

    private DefaultProxyChainAnalyzer analyzer = new DefaultProxyChainAnalyzer();

    private ProxyMonitorCollectorManager manager = mock(ProxyMonitorCollectorManager.class);

    private MetaCache metaCache = mock(MetaCache.class);

    private CrossDcKeeperContainerAddressService addressService = mock(CrossDcKeeperContainerAddressService.class);

    private EventMonitor eventMonitor = mock(EventMonitor.class);

    private List<DefaultTunnelInfo> tunnels = new ArrayList<>();

    private Map<KeeperMeta, String> keeper2Dc = new HashMap<>();

    private int tunnelSeq = 0;

    @Before
    public void beforeDefaultProxyChainAnalyzerTest() {
        ProxyMonitorCollector collector = mock(ProxyMonitorCollector.class);
        when(collector.getTunnelInfos()).thenAnswer(invocation -> tunnels);
        when(manager.getProxyMonitorResults()).thenReturn(Lists.newArrayList(collector));

        when(metaCache.isMetaChain(any(HostPort.class), any(HostPort.class))).thenReturn(true);
        when(metaCache.getKeeper2DcOfClusterShard(CLUSTER, SHARD)).thenReturn(keeper2Dc);

        analyzer.setExecutors(MoreExecutors.newDirectExecutorService());
        analyzer.setMetaCache(metaCache);
        analyzer.setProxyMonitorCollectorManager(manager);
        analyzer.setCrossDcKeeperContainerAddressService(addressService);
        analyzer.setEventMonitor(eventMonitor);
    }

    @Test
    public void testSourceDcViewDstIsRemoteContainerKeeper() {
        // 源机房 JQ 视角：dst 是远端 OY 容器 keeper 的 pod IP，meta 里只有 hostname:port
        HostPort networkDst = new HostPort("10.0.1.1", KEEPER_PORT);
        HostPort metaDst = new HostPort("kc-oy-1", KEEPER_PORT);
        mockMetaKeeper(metaDst, DC_OY);
        when(addressService.findHostnamesByIp("10.0.1.1")).thenReturn(Arrays.asList(new Pair<>(DC_OY, "kc-oy-1")));
        addFirstHopTunnel(DC_JQ, "10.2.0.1:40001", networkDst);

        analyzer.fullUpdate();

        assertChains(new DcClusterShardPeer(DC_JQ, CLUSTER, SHARD, DC_OY));
        // isMetaChain 用翻译后的 meta 地址；分片与机房取自翻译时拿到的 MetaDesc，不再按地址二次查找
        verify(metaCache).isMetaChain(HostPort.fromString("10.2.0.1:40001"), metaDst);
        verify(metaCache, never()).findClusterShard(any(HostPort.class));
        verify(metaCache, never()).getDc(any(HostPort.class));
        verify(eventMonitor, never()).logAlertEvent(anyString());
    }

    @Test
    public void testPeerDcViewSrcIsRemoteContainerKeeper() {
        // 目的机房 OY 视角：tunnel 在 peer 机房，只能按 src 网络 IP 找 backup 机房。
        // 10.0.2.2 是 JQ 容器 kc-jq-1 的次节点 pod IP，门面反查覆盖全部存活 latch 子节点
        HostPort dst = new HostPort("10.1.1.1", KEEPER_PORT);
        mockMetaKeeper(dst, DC_OY);
        keeper2Dc.put(physicalKeeper("10.1.1.1"), DC_OY.toLowerCase());
        keeper2Dc.put(containerKeeper("kc-jq-1"), DC_JQ.toLowerCase());
        when(addressService.findHostnamesByIp("10.0.2.2")).thenReturn(Arrays.asList(new Pair<>(DC_JQ, "kc-jq-1")));
        addNonFirstHopTunnel(DC_OY, "10.0.2.2:40001", dst);

        analyzer.fullUpdate();

        assertChains(new DcClusterShardPeer(DC_JQ, CLUSTER, SHARD, DC_OY));
        verify(metaCache, never()).getDcByIpAndClusterShard(anyString(), any());
        // dst 原始地址命中 meta，不做翻译
        verify(addressService, never()).findHostnamesByIp("10.1.1.1");
    }

    @Test
    public void testRelayDcViewAttributedToSourceDc() {
        // 多跳 JQ → RB(relay) → OY：RB 的 proxy 不是第一跳，不能把 RB 当 backup；按 src 归属得到源机房 JQ，
        // 与 JQ console 产出的 key 相同，collector 可合并出全链路
        HostPort dst = new HostPort("10.1.1.1", KEEPER_PORT);
        mockMetaKeeper(dst, DC_OY);
        keeper2Dc.put(physicalKeeper("10.3.0.1"), DC_JQ.toLowerCase());
        addNonFirstHopTunnel(DC_RB, "10.3.0.1:40001", dst);

        analyzer.fullUpdate();

        assertChains(new DcClusterShardPeer(DC_JQ, CLUSTER, SHARD, DC_OY));
    }

    @Test
    public void testRelayDcViewUnresolvedNotTakenAsBackup() {
        // relay 视角 src 归属零命中、兜底也查不到：跳过，而不是退回用 relay 机房当 backup
        HostPort dst = new HostPort("10.1.1.1", KEEPER_PORT);
        mockMetaKeeper(dst, DC_OY);
        addNonFirstHopTunnel(DC_RB, "10.3.0.1:40001", dst);

        analyzer.fullUpdate();

        assertChains();
        verify(metaCache).getDcByIpAndClusterShard("10.3.0.1", CLUSTER_SHARD);
    }

    @Test
    public void testFirstHopInSourceDcSkipsSrcAttribution() {
        // 第一跳直接取 tunnel 所在机房，不查 keeper 归属
        HostPort dst = new HostPort("10.1.1.1", KEEPER_PORT);
        mockMetaKeeper(dst, DC_OY);
        addFirstHopTunnel(DC_JQ, "10.3.0.1:40001", dst);

        analyzer.fullUpdate();

        assertChains(new DcClusterShardPeer(DC_JQ, CLUSTER, SHARD, DC_OY));
        verify(metaCache, never()).getKeeper2DcOfClusterShard(anyString(), anyString());
        verify(metaCache, never()).getDcByIpAndClusterShard(anyString(), any());
    }

    @Test
    public void testPhysicalKeeperChainUnchanged() {
        // 源机房视角：物理机 dst 直接命中，backup 机房来自 tunnel
        HostPort dst1 = new HostPort("10.1.1.1", KEEPER_PORT);
        mockMetaKeeper(dst1, DC_OY);
        addFirstHopTunnel(DC_JQ, "10.3.0.1:40001", dst1);

        // 目的机房视角：src 为物理机 keeper IP，按 IP 比较
        HostPort dst2 = new HostPort("10.1.1.2", KEEPER_PORT);
        mockMetaKeeper(dst2, DC_OY);
        keeper2Dc.put(physicalKeeper("10.3.0.2"), DC_RB.toLowerCase());
        addNonFirstHopTunnel(DC_OY, "10.3.0.2:40002", dst2);

        analyzer.fullUpdate();

        Map<DcClusterShardPeer, ProxyChain> chains = analyzer.getClusterShardChainMap();
        Assert.assertEquals(2, chains.size());
        Assert.assertTrue(chains.containsKey(new DcClusterShardPeer(DC_JQ, CLUSTER, SHARD, DC_OY)));
        Assert.assertTrue(chains.containsKey(new DcClusterShardPeer(DC_RB, CLUSTER, SHARD, DC_OY)));
        verify(addressService, never()).findHostnamesByIp("10.1.1.1");
        verify(addressService, never()).findHostnamesByIp("10.1.1.2");
        verify(metaCache, never()).getDcByIpAndClusterShard(anyString(), any());
    }

    @Test
    public void testSrcNoKeeperMatchFallsBackToRedis() {
        // 双向链路：src 不是任何 keeper，零匹配走 getDcByIpAndClusterShard 兜底
        HostPort dst = new HostPort("10.1.1.1", 6379);
        mockMetaKeeper(dst, DC_OY);
        when(metaCache.getDcByIpAndClusterShard("10.4.0.1", CLUSTER_SHARD)).thenReturn(DC_RB.toLowerCase());
        addNonFirstHopTunnel(DC_OY, "10.4.0.1:40001", dst);

        analyzer.fullUpdate();

        assertChains(new DcClusterShardPeer(DC_RB, CLUSTER, SHARD, DC_OY));
    }

    @Test
    public void testDstAmbiguousSkipped() {
        // 各机房 pod IP 段重叠：同一 IP 在 OY / RB 各对应一个容器，且两者都有 6380 的 keeper
        HostPort networkDst = new HostPort("10.0.1.1", KEEPER_PORT);
        mockMetaKeeper(new HostPort("kc-oy-1", KEEPER_PORT), DC_OY);
        mockMetaKeeper(new HostPort("kc-rb-1", KEEPER_PORT), DC_RB);
        when(addressService.findHostnamesByIp("10.0.1.1")).thenReturn(Arrays.asList(
                new Pair<>(DC_OY, "kc-oy-1"), new Pair<>(DC_RB, "kc-rb-1")));
        addFirstHopTunnel(DC_JQ, "10.2.0.1:40001", networkDst);

        analyzer.fullUpdate();

        assertChains();
        verify(metaCache, never()).isMetaChain(any(HostPort.class), any(HostPort.class));
        verify(eventMonitor, times(1)).logAlertEvent(ALERT_PROXY_CHAIN_KEEPER_AMBIGUOUS);
    }

    @Test
    public void testDstCandidateNotInMetaIgnored() {
        // 反查到两个容器，但只有一个在该端口上有 keeper：唯一命中，照常采用
        HostPort networkDst = new HostPort("10.0.1.1", KEEPER_PORT);
        mockMetaKeeper(new HostPort("kc-oy-1", KEEPER_PORT), DC_OY);
        when(addressService.findHostnamesByIp("10.0.1.1")).thenReturn(Arrays.asList(
                new Pair<>(DC_OY, "kc-oy-1"), new Pair<>(DC_RB, "kc-rb-1")));
        addFirstHopTunnel(DC_JQ, "10.2.0.1:40001", networkDst);

        analyzer.fullUpdate();

        assertChains(new DcClusterShardPeer(DC_JQ, CLUSTER, SHARD, DC_OY));
        verify(eventMonitor, never()).logAlertEvent(anyString());
    }

    @Test
    public void testSrcAmbiguousSkippedWithoutFallback() {
        HostPort dst = new HostPort("10.1.1.1", KEEPER_PORT);
        mockMetaKeeper(dst, DC_OY);
        keeper2Dc.put(containerKeeper("kc-jq-1"), DC_JQ.toLowerCase());
        keeper2Dc.put(containerKeeper("kc-rb-1"), DC_RB.toLowerCase());
        when(addressService.findHostnamesByIp("10.0.2.2")).thenReturn(Arrays.asList(
                new Pair<>(DC_JQ, "kc-jq-1"), new Pair<>(DC_RB, "kc-rb-1")));
        when(metaCache.getDcByIpAndClusterShard(anyString(), any())).thenReturn(DC_JQ.toLowerCase());
        addNonFirstHopTunnel(DC_OY, "10.0.2.2:40001", dst);

        analyzer.fullUpdate();

        assertChains();
        verify(metaCache, never()).getDcByIpAndClusterShard(anyString(), any());
        verify(eventMonitor, times(1)).logAlertEvent(ALERT_PROXY_CHAIN_KEEPER_AMBIGUOUS);
    }

    @Test
    public void testSrcSameIpAsPeerDcKeeperNotAttributedToPeer() {
        // src 的 pod IP 同时是 peer 机房 OY 该分片 keeper 所在容器的 IP：排除 peer 后唯一归到 JQ，不算多命中
        HostPort dst = new HostPort("10.1.1.1", KEEPER_PORT);
        mockMetaKeeper(dst, DC_OY);
        keeper2Dc.put(containerKeeper("kc-oy-1"), DC_OY.toLowerCase());
        keeper2Dc.put(containerKeeper("kc-jq-1"), DC_JQ.toLowerCase());
        when(addressService.findHostnamesByIp("10.0.2.2")).thenReturn(Arrays.asList(
                new Pair<>(DC_OY, "kc-oy-1"), new Pair<>(DC_JQ, "kc-jq-1")));
        addNonFirstHopTunnel(DC_OY, "10.0.2.2:40001", dst);

        analyzer.fullUpdate();

        assertChains(new DcClusterShardPeer(DC_JQ, CLUSTER, SHARD, DC_OY));
        verify(eventMonitor, never()).logAlertEvent(anyString());
    }

    @Test
    public void testSrcOnlyMatchesPeerDcKeeperNotAttributed() {
        // 唯一匹配的是 peer 机房自己的 keeper：排除后零匹配，走兜底；兜底也查不到则跳过
        HostPort dst = new HostPort("10.1.1.1", KEEPER_PORT);
        mockMetaKeeper(dst, DC_OY);
        keeper2Dc.put(containerKeeper("kc-oy-1"), DC_OY.toLowerCase());
        when(addressService.findHostnamesByIp("10.0.2.2")).thenReturn(Arrays.asList(new Pair<>(DC_OY, "kc-oy-1")));
        addNonFirstHopTunnel(DC_OY, "10.0.2.2:40001", dst);

        analyzer.fullUpdate();

        assertChains();
        verify(metaCache).getDcByIpAndClusterShard("10.0.2.2", CLUSTER_SHARD);
    }

    @Test
    public void testAmbiguousAlertOncePerRound() {
        // dst 多命中一条 + src 多命中一条，本轮只告警一次；另有一条正常链路不受影响
        HostPort networkDst = new HostPort("10.0.1.1", KEEPER_PORT);
        mockMetaKeeper(new HostPort("kc-oy-1", KEEPER_PORT), DC_OY);
        mockMetaKeeper(new HostPort("kc-rb-1", KEEPER_PORT), DC_RB);
        when(addressService.findHostnamesByIp("10.0.1.1")).thenReturn(Arrays.asList(
                new Pair<>(DC_OY, "kc-oy-1"), new Pair<>(DC_RB, "kc-rb-1")));
        addFirstHopTunnel(DC_JQ, "10.2.0.1:40001", networkDst);

        HostPort dst = new HostPort("10.1.1.1", KEEPER_PORT);
        mockMetaKeeper(dst, DC_OY);
        keeper2Dc.put(containerKeeper("kc-jq-1"), DC_JQ.toLowerCase());
        keeper2Dc.put(containerKeeper("kc-rb-2"), DC_RB.toLowerCase());
        when(addressService.findHostnamesByIp("10.0.2.2")).thenReturn(Arrays.asList(
                new Pair<>(DC_JQ, "kc-jq-1"), new Pair<>(DC_RB, "kc-rb-2")));
        addNonFirstHopTunnel(DC_OY, "10.0.2.2:40001", dst);

        HostPort normalDst = new HostPort("10.1.1.2", KEEPER_PORT);
        mockMetaKeeper(normalDst, DC_OY);
        addFirstHopTunnel(DC_RB, "10.3.0.1:40001", normalDst);

        analyzer.fullUpdate();
        assertChains(new DcClusterShardPeer(DC_RB, CLUSTER, SHARD, DC_OY));
        verify(eventMonitor, times(1)).logAlertEvent(anyString());
        verify(eventMonitor, times(1)).logAlertEvent(ALERT_PROXY_CHAIN_KEEPER_AMBIGUOUS);

        // 条件持续则每轮都报
        analyzer.fullUpdate();
        verify(eventMonitor, times(2)).logAlertEvent(ALERT_PROXY_CHAIN_KEEPER_AMBIGUOUS);
    }

    @Test
    public void testNoAmbiguousNoAlert() {
        HostPort dst = new HostPort("10.1.1.1", KEEPER_PORT);
        mockMetaKeeper(dst, DC_OY);
        addFirstHopTunnel(DC_JQ, "10.3.0.1:40001", dst);

        analyzer.fullUpdate();
        analyzer.fullUpdate();

        assertChains(new DcClusterShardPeer(DC_JQ, CLUSTER, SHARD, DC_OY));
        verify(eventMonitor, never()).logAlertEvent(anyString());
    }

    @Test
    public void testDstUnresolvedSkipped() {
        // 远端快照尚未拉到（首轮前或新 pod）：dst 翻译零命中，跳过且不告警
        HostPort networkDst = new HostPort("10.0.1.1", KEEPER_PORT);
        addFirstHopTunnel(DC_JQ, "10.2.0.1:40001", networkDst);

        analyzer.fullUpdate();

        assertChains();
        verify(addressService).findHostnamesByIp("10.0.1.1");
        verify(metaCache, never()).isMetaChain(any(HostPort.class), any(HostPort.class));
        verify(eventMonitor, never()).logAlertEvent(anyString());
    }

    private void assertChains(DcClusterShardPeer... expected) {
        Map<DcClusterShardPeer, ProxyChain> chains = analyzer.getClusterShardChainMap();
        logger.info("[chains] {}", chains);
        Assert.assertEquals(new HashSet<>(Arrays.asList(expected)), chains.keySet());
    }

    /** 只 mock {@code findMetaDesc}：分片与机房由 analyzer 从 MetaDesc 直接取，不再按地址二次查找 */
    private void mockMetaKeeper(HostPort metaAddress, String dc) {
        when(metaCache.findMetaDesc(metaAddress)).thenReturn(new XpipeMetaManager.MetaDesc(new DcMeta(dc),
                new ClusterMeta(CLUSTER), new ShardMeta(SHARD), new KeeperMeta().setPort(metaAddress.getPort())));
    }

    private KeeperMeta physicalKeeper(String ip) {
        return new KeeperMeta().setIp(ip).setPort(KEEPER_PORT);
    }

    private KeeperMeta containerKeeper(String hostname) {
        return new KeeperMeta().setIp("0.0.0.0").setPort(KEEPER_PORT).setHostname(hostname);
    }

    /** 第一跳 proxy 上的 tunnel：frontend 对端就是 source（链路源端 keeper / redis） */
    private void addFirstHopTunnel(String proxyDc, String src, HostPort dst) {
        addTunnel(proxyDc, src, src, dst);
    }

    /** 非第一跳（peer / relay）proxy 上的 tunnel：frontend 对端是上一跳 proxy */
    private void addNonFirstHopTunnel(String proxyDc, String src, HostPort dst) {
        addTunnel(proxyDc, src, "10.9.9." + (tunnelSeq + 1) + ":443", dst);
    }

    /** tunnelId 按 {@code TunnelIdentity.toString()} 真实格式：{@code {source}-R(frontend 对端)-L(..)->R(..)-{dest}} */
    private void addTunnel(String proxyDc, String src, String frontendRemote, HostPort dst) {
        int seq = ++tunnelSeq;
        String tunnelId = String.format("%s-R(%s)-L(127.0.0.%d:%d)->R(127.0.0.%d:443)-TCP://%s:%d",
                src, frontendRemote, seq, 10000 + seq, seq, dst.getHost(), dst.getPort());
        tunnels.add(new DefaultTunnelInfo(getProxy(proxyDc), tunnelId)
                .setTunnelSocketStatsResult(genTunnelSSR(tunnelId)).setTunnelStatsResult(genTunnelSR(tunnelId)));
    }
}
