package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorShardMeta;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.Arrays;
import java.util.Set;

/**
 * @author lishanglin
 * date 2020/12/31
 */
public class BeaconMetaServiceImplTest extends AbstractConsoleIntegrationTest {

    private MetaCache metaCache;

    private ConsoleCommonConfig config;

    private BeaconMetaServiceImpl beaconMetaService;

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/beacon-migration-test.sql");
    }

    @Before
    public void setupBeaconMetaServiceImplTest() {
        metaCache = Mockito.mock(MetaCache.class);
        config = Mockito.mock(ConsoleCommonConfig.class);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
        Mockito.doAnswer(invocation -> {
            String activeDc = invocation.getArgument(0, String.class);
            String backupDc = invocation.getArgument(1, String.class);
            XpipeMeta xpipeMeta = getXpipeMeta();
            logger.info("[setupBeaconMetaServiceImplTest] {}", activeDc);
            return !xpipeMeta.getDcs().get(activeDc).getZone().equals(xpipeMeta.getDcs().get(backupDc).getZone());
        }).when(metaCache).isCrossRegion(Mockito.anyString(), Mockito.anyString());

        Mockito.when(config.getBeaconSupportZones()).thenReturn(Collections.singleton("SHA"));
        Mockito.when(metaCache.isDcInRegion(Mockito.anyString(), Mockito.eq("SHA"))).thenReturn(true);
        Mockito.when(metaCache.getActiveDc("cluster1")).thenReturn("jq");

        beaconMetaService = new BeaconMetaServiceImpl(metaCache, config);
    }

    @Test
    public void testBuildDrBeaconGroups() {
        Set<MonitorGroupMeta> groups = beaconMetaService.buildDrBeaconGroups("cluster1", "jq");
        logger.info("[testBuildDrBeaconGroups] {}", groups);
        Assert.assertEquals(expectedBeaconGroups(), groups);
    }

    @Test
    public void testCompareDrBeaconMetaWithXPipe() {
        Assert.assertTrue(beaconMetaService.compareDrBeaconMetaWithXPipe("cluster1", expectedBeaconGroups()));
    }

    private XpipeMeta upperCaseMetaFrom(XpipeMeta source) {
        XpipeMeta upperCaseMeta = new XpipeMeta();
        source.getDcs().forEach((dc, dcMeta) -> {
            DcMeta upperDcMeta = new DcMeta(dc.toUpperCase()).setZone(dcMeta.getZone());
            dcMeta.getClusters().values().forEach(clusterMeta ->
                    upperDcMeta.addCluster(copyClusterMeta(clusterMeta)));
            upperCaseMeta.addDc(upperDcMeta);
        });
        return upperCaseMeta;
    }

    private ClusterMeta copyClusterMeta(ClusterMeta source) {
        ClusterMeta clusterMeta = new ClusterMeta(source.getId());
        clusterMeta.setType(source.getType());
        clusterMeta.setAzGroupType(source.getAzGroupType());
        clusterMeta.setActiveDc(source.getActiveDc() == null ? null : source.getActiveDc().toUpperCase());
        clusterMeta.setBackupDcs(source.getBackupDcs() == null ? null : source.getBackupDcs().toUpperCase());
        source.getShards().values().forEach(clusterMeta::addShard);
        return clusterMeta;
    }

    @Test
    public void testBuildDrBeaconGroupsWithUpperCaseDcKeys() {
        XpipeMeta upperCaseMeta = upperCaseMetaFrom(getXpipeMeta());

        Mockito.when(metaCache.getXpipeMeta()).thenReturn(upperCaseMeta);
        Mockito.doAnswer(invocation -> {
            String activeDc = invocation.getArgument(0, String.class);
            String backupDc = invocation.getArgument(1, String.class);
            return !upperCaseMeta.getDcs().get(activeDc.toUpperCase()).getZone()
                    .equals(upperCaseMeta.getDcs().get(backupDc.toUpperCase()).getZone());
        }).when(metaCache).isCrossRegion(Mockito.anyString(), Mockito.anyString());

        Set<MonitorGroupMeta> groups = beaconMetaService.buildDrBeaconGroups("cluster1", "jq");
        Assert.assertFalse(groups.isEmpty());
        Assert.assertEquals(4, groups.size());
        Assert.assertTrue(groups.stream().allMatch(group ->
                "JQ".equals(group.getIdc()) || "OY".equals(group.getIdc())));
        Assert.assertTrue(groups.stream().allMatch(group ->
                group.getName().endsWith("+JQ") || group.getName().endsWith("+OY")));

        Set<MonitorGroupMeta> expected = upperCaseExpectedBeaconGroups();
        Assert.assertEquals(expected, groups);
        Assert.assertTrue(beaconMetaService.compareDrBeaconMetaWithXPipe("cluster1", "jq", expected));
        Assert.assertTrue(beaconMetaService.compareDrBeaconMetaWithXPipe("cluster1", "JQ", expected));
    }

    @Test
    public void testBuildSentinelBeaconShardsWithUpperCaseDcKey() {
        XpipeMeta upperCaseMeta = upperCaseMetaFrom(getXpipeMeta());
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(upperCaseMeta);

        Set<MonitorShardMeta> shards = beaconMetaService.buildSentinelBeaconShards("cluster1", "jq", Collections.emptyMap());
        Assert.assertFalse(shards.isEmpty());
        shards.forEach(shard -> shard.getGroups().forEach(group -> Assert.assertEquals("JQ", group.getIdc())));
    }

    @Test
    public void testBuildSentinelBeaconShards() {
        Set<MonitorShardMeta> shards = beaconMetaService.buildSentinelBeaconShards("cluster1", "jq", Collections.emptyMap());
        Assert.assertEquals(expectedBeaconShards(), shards);
    }

    @Test
    public void testBuildSentinelBeaconShardsWithPublishMasters() {
        Set<MonitorShardMeta> shards = beaconMetaService.buildSentinelBeaconShards("cluster1", "jq",
                Collections.singletonMap("shard1", HostPort.fromString("127.0.0.1:6380")));

        MonitorShardMeta shard1 = shards.stream()
                .filter(shard -> "shard1".equals(shard.getName()))
                .findFirst()
                .get();

        Assert.assertTrue(shard1.getGroups().stream()
                .filter(group -> group.getNodes().contains(HostPort.fromString("127.0.0.1:6380")))
                .findFirst()
                .get()
                .isMasterGroup());
        Assert.assertFalse(shard1.getGroups().stream()
                .filter(group -> group.getNodes().contains(HostPort.fromString("127.0.0.1:6379")))
                .findFirst()
                .get()
                .isMasterGroup());
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "multi-zone-meta.xml";
    }

    private Set<MonitorGroupMeta> expectedBeaconGroups() {
        return Sets.newHashSet(
                new MonitorGroupMeta("shard1+jq", "jq", Sets.newHashSet(HostPort.fromString("127.0.0.1:6379"), HostPort.fromString("127.0.0.1:6380")), true),
                new MonitorGroupMeta("shard2+jq", "jq", Sets.newHashSet(HostPort.fromString("127.0.0.1:6381"), HostPort.fromString("127.0.0.1:6382")), true),
                new MonitorGroupMeta("shard1+oy", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6383"), HostPort.fromString("127.0.0.1:6384")), false),
                new MonitorGroupMeta("shard2+oy", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6385"), HostPort.fromString("127.0.0.1:6386")), false)
        );
    }

    private Set<MonitorGroupMeta> upperCaseExpectedBeaconGroups() {
        return Sets.newHashSet(
                new MonitorGroupMeta("shard1+JQ", "JQ", Sets.newHashSet(HostPort.fromString("127.0.0.1:6379"), HostPort.fromString("127.0.0.1:6380")), true),
                new MonitorGroupMeta("shard2+JQ", "JQ", Sets.newHashSet(HostPort.fromString("127.0.0.1:6381"), HostPort.fromString("127.0.0.1:6382")), true),
                new MonitorGroupMeta("shard1+OY", "OY", Sets.newHashSet(HostPort.fromString("127.0.0.1:6383"), HostPort.fromString("127.0.0.1:6384")), false),
                new MonitorGroupMeta("shard2+OY", "OY", Sets.newHashSet(HostPort.fromString("127.0.0.1:6385"), HostPort.fromString("127.0.0.1:6386")), false)
        );
    }

    private Set<MonitorShardMeta> expectedBeaconShards() {
        return Sets.newHashSet(
                new MonitorShardMeta("shard1", Arrays.asList(
                        new MonitorGroupMeta("127.0.0.1:6379", "jq",
                                Sets.newHashSet(HostPort.fromString("127.0.0.1:6379")), true),
                        new MonitorGroupMeta("127.0.0.1:6380", "jq",
                                Sets.newHashSet(HostPort.fromString("127.0.0.1:6380")), false)
                )),
                new MonitorShardMeta("shard2", Arrays.asList(
                        new MonitorGroupMeta("127.0.0.1:6381", "jq",
                                Sets.newHashSet(HostPort.fromString("127.0.0.1:6381")), true),
                        new MonitorGroupMeta("127.0.0.1:6382", "jq",
                                Sets.newHashSet(HostPort.fromString("127.0.0.1:6382")), false)
                ))
        );
    }

}
