package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.console.service.CrossDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.entity.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.when;

public class CheckerKeeperIpFillerTest {

    private static final String PLACEHOLDER_IP = "0.0.0.0";

    private CrossDcKeeperContainerAddressService addressService;

    private CheckerKeeperIpFiller filler;

    @Before
    public void setUp() {
        addressService = Mockito.mock(CrossDcKeeperContainerAddressService.class);
        filler = new CheckerKeeperIpFiller();
        filler.addressService = addressService;
    }

    @Test
    public void testFill_resolvesContainerKeeperIp() {
        KeeperMeta containerKeeper = new KeeperMeta()
                .setIp(PLACEHOLDER_IP).setPort(6380).setHostname("pod-1");
        ShardMeta shard = new ShardMeta("shard1").addKeeper(containerKeeper);
        ClusterMeta cluster = new ClusterMeta("cluster1").addShard(shard);
        DcMeta dc = new DcMeta("jq").addCluster(cluster);
        XpipeMeta source = new XpipeMeta().addDc(dc);

        when(addressService.getAllHostnameToIp("jq"))
                .thenReturn(Collections.singletonMap("pod-1", "10.0.0.1"));

        XpipeMeta result = filler.fill(source);

        KeeperMeta filled = result.getDcs().get("jq").getClusters().get("cluster1")
                .getShards().get("shard1").getKeepers().get(0);
        Assert.assertEquals("10.0.0.1", filled.getIp());
        Assert.assertEquals("pod-1", filled.getHostname());
    }

    @Test
    public void testFill_keepsPlaceholderWhenUnresolved() {
        KeeperMeta containerKeeper = new KeeperMeta()
                .setIp(PLACEHOLDER_IP).setPort(6380).setHostname("pod-1");
        ShardMeta shard = new ShardMeta("shard1").addKeeper(containerKeeper);
        ClusterMeta cluster = new ClusterMeta("cluster1").addShard(shard);
        DcMeta dc = new DcMeta("jq").addCluster(cluster);
        XpipeMeta source = new XpipeMeta().addDc(dc);

        when(addressService.getAllHostnameToIp("jq")).thenReturn(Collections.emptyMap());

        XpipeMeta result = filler.fill(source);

        KeeperMeta filled = result.getDcs().get("jq").getClusters().get("cluster1")
                .getShards().get("shard1").getKeepers().get(0);
        Assert.assertEquals(PLACEHOLDER_IP, filled.getIp());
    }

    @Test
    public void testFill_physicalKeeperUntouchedAndNotQueried() {
        KeeperMeta physicalKeeper = new KeeperMeta()
                .setIp("10.0.0.99").setPort(6380).setHostname(null);
        ShardMeta shard = new ShardMeta("shard1").addKeeper(physicalKeeper);
        ClusterMeta cluster = new ClusterMeta("cluster1").addShard(shard);
        DcMeta dc = new DcMeta("jq").addCluster(cluster);
        XpipeMeta source = new XpipeMeta().addDc(dc);

        when(addressService.getAllHostnameToIp("jq")).thenReturn(Collections.emptyMap());

        XpipeMeta result = filler.fill(source);

        KeeperMeta filled = result.getDcs().get("jq").getClusters().get("cluster1")
                .getShards().get("shard1").getKeepers().get(0);
        Assert.assertEquals("10.0.0.99", filled.getIp());
        Assert.assertNull(filled.getHostname());
        // shard without container keeper is shared by reference
        Assert.assertSame(shard, result.getDcs().get("jq").getClusters().get("cluster1").getShards().get("shard1"));
    }

    @Test
    public void testFill_sourceNotMutated() {
        KeeperMeta containerKeeper = new KeeperMeta()
                .setIp(PLACEHOLDER_IP).setPort(6380).setHostname("pod-1");
        ShardMeta shard = new ShardMeta("shard1").addKeeper(containerKeeper);
        ClusterMeta cluster = new ClusterMeta("cluster1").addShard(shard);
        DcMeta dc = new DcMeta("jq").addCluster(cluster);
        XpipeMeta source = new XpipeMeta().addDc(dc);

        when(addressService.getAllHostnameToIp("jq"))
                .thenReturn(Collections.singletonMap("pod-1", "10.0.0.1"));

        filler.fill(source);

        // source keeper ip unchanged
        Assert.assertEquals(PLACEHOLDER_IP, source.getDcs().get("jq").getClusters().get("cluster1")
                .getShards().get("shard1").getKeepers().get(0).getIp());
    }

    @Test
    public void testFill_addressServiceExceptionDoesNotFail() {
        KeeperMeta containerKeeper = new KeeperMeta()
                .setIp(PLACEHOLDER_IP).setPort(6380).setHostname("pod-1");
        ShardMeta shard = new ShardMeta("shard1").addKeeper(containerKeeper);
        ClusterMeta cluster = new ClusterMeta("cluster1").addShard(shard);
        DcMeta dc = new DcMeta("jq").addCluster(cluster);
        XpipeMeta source = new XpipeMeta().addDc(dc);

        when(addressService.getAllHostnameToIp("jq"))
                .thenThrow(new RuntimeException("zk down"));

        XpipeMeta result = filler.fill(source);

        // returns a copy with placeholder ip, no exception thrown
        KeeperMeta filled = result.getDcs().get("jq").getClusters().get("cluster1")
                .getShards().get("shard1").getKeepers().get(0);
        Assert.assertEquals(PLACEHOLDER_IP, filled.getIp());
    }

    @Test
    public void testFill_mixedKeeperInSameShard() {
        KeeperMeta containerKeeper = new KeeperMeta()
                .setIp(PLACEHOLDER_IP).setPort(6380).setHostname("pod-1");
        KeeperMeta physicalKeeper = new KeeperMeta()
                .setIp("10.0.0.99").setPort(6381).setHostname(null);
        RedisMeta redis = new RedisMeta().setIp("10.0.0.50").setPort(6379);
        ShardMeta shard = new ShardMeta("shard1")
                .addKeeper(containerKeeper)
                .addKeeper(physicalKeeper)
                .addRedis(redis);
        ClusterMeta cluster = new ClusterMeta("cluster1").addShard(shard);
        DcMeta dc = new DcMeta("jq").addCluster(cluster);
        XpipeMeta source = new XpipeMeta().addDc(dc);

        when(addressService.getAllHostnameToIp("jq"))
                .thenReturn(Collections.singletonMap("pod-1", "10.0.0.1"));

        XpipeMeta result = filler.fill(source);

        java.util.List<KeeperMeta> keepers = result.getDcs().get("jq").getClusters().get("cluster1")
                .getShards().get("shard1").getKeepers();
        Assert.assertEquals("10.0.0.1", keepers.get(0).getIp());
        Assert.assertEquals("10.0.0.99", keepers.get(1).getIp());
        // redis shared by reference
        Assert.assertSame(redis, result.getDcs().get("jq").getClusters().get("cluster1")
                .getShards().get("shard1").getRedises().get(0));
    }

    @Test
    public void testFill_multipleDcs() {
        KeeperMeta keeperJq = new KeeperMeta().setIp(PLACEHOLDER_IP).setPort(6380).setHostname("pod-jq");
        DcMeta dcJq = new DcMeta("jq").addCluster(
                new ClusterMeta("cluster1").addShard(new ShardMeta("shard1").addKeeper(keeperJq)));
        KeeperMeta keeperOy = new KeeperMeta().setIp(PLACEHOLDER_IP).setPort(6380).setHostname("pod-oy");
        DcMeta dcOy = new DcMeta("oy").addCluster(
                new ClusterMeta("cluster1").addShard(new ShardMeta("shard1").addKeeper(keeperOy)));
        XpipeMeta source = new XpipeMeta().addDc(dcJq).addDc(dcOy);

        Map<String, String> jqMap = new HashMap<>();
        jqMap.put("pod-jq", "10.0.0.1");
        Map<String, String> oyMap = new HashMap<>();
        oyMap.put("pod-oy", "10.0.0.2");
        when(addressService.getAllHostnameToIp("jq")).thenReturn(jqMap);
        when(addressService.getAllHostnameToIp("oy")).thenReturn(oyMap);

        XpipeMeta result = filler.fill(source);

        Assert.assertEquals("10.0.0.1", result.getDcs().get("jq").getClusters().get("cluster1")
                .getShards().get("shard1").getKeepers().get(0).getIp());
        Assert.assertEquals("10.0.0.2", result.getDcs().get("oy").getClusters().get("cluster1")
                .getShards().get("shard1").getKeepers().get(0).getIp());
    }
}
