package com.ctrip.xpipe.api.migration;

import com.ctrip.xpipe.api.migration.auto.data.MonitorClusterMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorShardMeta;
import com.ctrip.xpipe.endpoint.HostPort;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MonitorClusterMetaTest {

    @Test
    public void testMonitorClusterMeta(){
        Set<MonitorGroupMeta> group1 = getMonitorGroupMeta1();
        MonitorClusterMeta monitorClusterMeta1 = new MonitorClusterMeta(group1);

        Set<MonitorGroupMeta> group2 = getMonitorGroupMeta2();
        MonitorClusterMeta monitorClusterMeta2 = new MonitorClusterMeta(group2);

        Set<MonitorGroupMeta> group3 = getMonitorGroupMeta3();
        MonitorClusterMeta monitorClusterMeta3 = new MonitorClusterMeta(group3);

        Set<MonitorGroupMeta> group4 = getMonitorGroupMeta4();
        MonitorClusterMeta monitorClusterMeta4 = new MonitorClusterMeta(group4);

        Set<MonitorGroupMeta> group5 = getMonitorGroupMeta5();
        MonitorClusterMeta monitorClusterMeta5 = new MonitorClusterMeta(group5);

        Set<MonitorGroupMeta> group6 = getMonitorGroupMeta6();
        MonitorClusterMeta monitorClusterMeta6 = new MonitorClusterMeta(group6);

        Assert.assertEquals(monitorClusterMeta1.generateHashCodeForBeaconCheck(false), monitorClusterMeta2.generateHashCodeForBeaconCheck(false));
        Assert.assertNotEquals(monitorClusterMeta1.generateHashCodeForBeaconCheck(false), monitorClusterMeta3.generateHashCodeForBeaconCheck(false));
        Assert.assertNotEquals(monitorClusterMeta1.generateHashCodeForBeaconCheck(false), monitorClusterMeta4.generateHashCodeForBeaconCheck(false));
        Assert.assertNotEquals(monitorClusterMeta4.generateHashCodeForBeaconCheck(false), monitorClusterMeta5.generateHashCodeForBeaconCheck(false));
        Assert.assertNotEquals(monitorClusterMeta5.generateHashCodeForBeaconCheck(false), monitorClusterMeta6.generateHashCodeForBeaconCheck(false));
        System.out.println(monitorClusterMeta1.generateHashCodeForBeaconCheck(false));
        System.out.println(monitorClusterMeta2.generateHashCodeForBeaconCheck(false));
        System.out.println(monitorClusterMeta3.generateHashCodeForBeaconCheck(false));
        System.out.println(monitorClusterMeta4.generateHashCodeForBeaconCheck(false));
        System.out.println(monitorClusterMeta5.generateHashCodeForBeaconCheck(false));
        System.out.println(monitorClusterMeta6.generateHashCodeForBeaconCheck(false));
    }


    private Set<MonitorGroupMeta> getMonitorGroupMeta1() {
        return Sets.newHashSet(
                new MonitorGroupMeta("shard1+jq", "jq", Sets.newHashSet(HostPort.fromString("127.0.0.1:6379"), HostPort.fromString("127.0.0.1:6380")), true),
                new MonitorGroupMeta("shard2+jq", "jq", Sets.newHashSet(HostPort.fromString("127.0.0.1:6381"), HostPort.fromString("127.0.0.1:6382")), true),
                new MonitorGroupMeta("shard1+oy", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6383"), HostPort.fromString("127.0.0.1:6384")), false),
                new MonitorGroupMeta("shard2+oy", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6385"), HostPort.fromString("127.0.0.1:6386")), false)
        );
    }

    private Set<MonitorGroupMeta> getMonitorGroupMeta2() {
        return Sets.newHashSet(
                new MonitorGroupMeta("shard1+oy", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6383"), HostPort.fromString("127.0.0.1:6384")), false),
                new MonitorGroupMeta("shard1+jq", "jq", Sets.newHashSet(HostPort.fromString("127.0.0.1:6379"), HostPort.fromString("127.0.0.1:6380")), true),
                new MonitorGroupMeta("shard2+jq", "jq", Sets.newHashSet(HostPort.fromString("127.0.0.1:6381"), HostPort.fromString("127.0.0.1:6382")), true),
                new MonitorGroupMeta("shard2+oy", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6385"), HostPort.fromString("127.0.0.1:6386")), false)
        );
    }

    private Set<MonitorGroupMeta> getMonitorGroupMeta3() {
        return Sets.newHashSet(
                new MonitorGroupMeta("shard1+jq", "jq", Sets.newHashSet(HostPort.fromString("127.0.0.1:6379"), HostPort.fromString("127.0.0.1:6380")), false),
                new MonitorGroupMeta("shard2+jq", "jq", Sets.newHashSet(HostPort.fromString("127.0.0.1:6381"), HostPort.fromString("127.0.0.1:6382")), false),
                new MonitorGroupMeta("shard1+oy", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6383"), HostPort.fromString("127.0.0.1:6384")), true),
                new MonitorGroupMeta("shard2+oy", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6385"), HostPort.fromString("127.0.0.1:6386")), true)
        );
    }

    private Set<MonitorGroupMeta> getMonitorGroupMeta4() {
        return Sets.newHashSet(
                new MonitorGroupMeta("shard1+jq", "jq", Sets.newHashSet(HostPort.fromString("127.0.0.1:6379"), HostPort.fromString("127.0.0.1:6380")), true),
                new MonitorGroupMeta("shard2+jq", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6381"), HostPort.fromString("127.0.0.1:6382")), true),
                new MonitorGroupMeta("shard1+oy", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6383"), HostPort.fromString("127.0.0.1:6384")), false),
                new MonitorGroupMeta("shard2+oy", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6385"), HostPort.fromString("127.0.0.1:6386")), false)
        );
    }

    private Set<MonitorGroupMeta> getMonitorGroupMeta5() {
        return Sets.newHashSet(
                new MonitorGroupMeta("shard1+jq", "jq", Sets.newHashSet(HostPort.fromString("127.0.0.1:6379"), HostPort.fromString("127.0.0.1:6382")), true),
                new MonitorGroupMeta("shard2+jq", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6381"), HostPort.fromString("127.0.0.1:6380")), true),
                new MonitorGroupMeta("shard1+oy", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6383"), HostPort.fromString("127.0.0.1:6384")), false),
                new MonitorGroupMeta("shard2+oy", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6385"), HostPort.fromString("127.0.0.1:6386")), false)
        );
    }

    private Set<MonitorGroupMeta> getMonitorGroupMeta6() {
        return Sets.newHashSet(
                new MonitorGroupMeta("shard1+jq", "jq", Sets.newHashSet(HostPort.fromString("127.0.0.1:6379"), HostPort.fromString("127.0.0.1:6382")), false),
                new MonitorGroupMeta("shard2+jq", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6381"), HostPort.fromString("127.0.0.1:6380")), false),
                new MonitorGroupMeta("shard1+oy", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6383"), HostPort.fromString("127.0.0.1:6384")), false),
                new MonitorGroupMeta("shard2+oy", "oy", Sets.newHashSet(HostPort.fromString("127.0.0.1:6385"), HostPort.fromString("127.0.0.1:6386")), false)
        );
    }

    @Test
    public void testMonitorClusterMetaWithShardsHash() {
        MonitorGroupMeta master = new MonitorGroupMeta("127.0.0.1:6379", "jq",
                Sets.newHashSet(HostPort.fromString("127.0.0.1:6379")), true);
        MonitorGroupMeta slave = new MonitorGroupMeta("127.0.0.1:6380", "jq",
                Sets.newHashSet(HostPort.fromString("127.0.0.1:6380")), false);
        Set<MonitorGroupMeta> groups = Sets.newHashSet(
                new MonitorGroupMeta(master.getName(), master.getIdc(), master.getNodes(), false),
                new MonitorGroupMeta(slave.getName(), slave.getIdc(), slave.getNodes(), false)
        );

        MonitorShardMeta shardMeta = new MonitorShardMeta("shard1", java.util.Arrays.asList(master, slave));
        MonitorClusterMeta fromShards = new MonitorClusterMeta(null, Collections.singleton(shardMeta), null);
        MonitorClusterMeta fromGroups = new MonitorClusterMeta(groups);

        Assert.assertEquals(fromGroups.generateHashCodeForBeaconCheck(false), fromShards.generateHashCodeForBeaconCheck(false));
    }

    @Test
    public void testGenerateHashCode_withAzVsWithoutAz_notEqual() {
        MonitorGroupMeta withAz = new MonitorGroupMeta("shard1+jq", "jq",
                Sets.newHashSet(HostPort.fromString("127.0.0.1:6379")), true);
        withAz.setAz("az1");

        MonitorGroupMeta withoutAz = new MonitorGroupMeta("shard1+jq", "jq",
                Sets.newHashSet(HostPort.fromString("127.0.0.1:6379")), true);

        MonitorClusterMeta clusterWithAz    = new MonitorClusterMeta(Sets.newHashSet(withAz));
        MonitorClusterMeta clusterWithoutAz = new MonitorClusterMeta(Sets.newHashSet(withoutAz));

        Assert.assertNotEquals(clusterWithAz.generateHashCodeForBeaconCheck(false), clusterWithoutAz.generateHashCodeForBeaconCheck(false));
    }

    @Test
    public void testGenerateHashCode_bothWithSameAz_equal() {
        MonitorGroupMeta groupA = new MonitorGroupMeta("shard1+jq", "jq",
                Sets.newHashSet(HostPort.fromString("127.0.0.1:6379")), true);
        groupA.setAz("az1");

        MonitorGroupMeta groupB = new MonitorGroupMeta("shard1+jq", "jq",
                Sets.newHashSet(HostPort.fromString("127.0.0.1:6379")), true);
        groupB.setAz("az1");

        MonitorClusterMeta clusterA = new MonitorClusterMeta(Sets.newHashSet(groupA));
        MonitorClusterMeta clusterB = new MonitorClusterMeta(Sets.newHashSet(groupB));

        Assert.assertEquals(clusterA.generateHashCodeForBeaconCheck(false), clusterB.generateHashCodeForBeaconCheck(false));
    }

    @Test
    public void testGenerateHashCode_bothWithNullAz_equal() {
        MonitorGroupMeta groupA = new MonitorGroupMeta("shard1+jq", "jq",
                Sets.newHashSet(HostPort.fromString("127.0.0.1:6379")), true);

        MonitorGroupMeta groupB = new MonitorGroupMeta("shard1+jq", "jq",
                Sets.newHashSet(HostPort.fromString("127.0.0.1:6379")), true);

        MonitorClusterMeta clusterA = new MonitorClusterMeta(Sets.newHashSet(groupA));
        MonitorClusterMeta clusterB = new MonitorClusterMeta(Sets.newHashSet(groupB));

        Assert.assertEquals(clusterA.generateHashCodeForBeaconCheck(false), clusterB.generateHashCodeForBeaconCheck(false));
    }

    @Test
    public void testGenerateHashCode_sameGroupsDifferentInsertOrder_equal() {
        MonitorGroupMeta gm1 = new MonitorGroupMeta("shard1+jq", "jq",
                Sets.newHashSet(HostPort.fromString("127.0.0.1:6379")), true);
        gm1.setAz("az1");

        MonitorGroupMeta gm2 = new MonitorGroupMeta("shard2+oy", "oy",
                Sets.newHashSet(HostPort.fromString("127.0.0.1:6380")), false);
        gm2.setAz("az2");

        MonitorClusterMeta cluster12 = new MonitorClusterMeta(Sets.newHashSet(gm1, gm2));
        MonitorClusterMeta cluster21 = new MonitorClusterMeta(Sets.newHashSet(gm2, gm1));

        Assert.assertEquals(cluster12.generateHashCodeForBeaconCheck(false), cluster21.generateHashCodeForBeaconCheck(false));
    }


    @Test
    public void testMonitorClusterMetaWithExtraHash() {
        Set<MonitorGroupMeta> groups = getMonitorGroupMeta1();

        Map<String, String> extra1 = new HashMap<>();
        extra1.put("lastModifyTime", "20200101103030001");
        extra1.put("region", "xy");
        MonitorClusterMeta withExtra1 = new MonitorClusterMeta(groups, extra1);

        Map<String, String> extra2 = new HashMap<>();
        extra2.put("region", "xy");
        extra2.put("lastModifyTime", "20200101103030001");
        MonitorClusterMeta withExtra2 = new MonitorClusterMeta(groups, extra2);

        Map<String, String> extra3 = new HashMap<>();
        extra3.put("lastModifyTime", "20200101103030002");
        extra3.put("region", "xy");
        MonitorClusterMeta withExtra3 = new MonitorClusterMeta(groups, extra3);

        MonitorClusterMeta withoutExtra = new MonitorClusterMeta(groups);
        MonitorClusterMeta withNullExtra = new MonitorClusterMeta(groups, null);
        MonitorClusterMeta withEmptyExtra = new MonitorClusterMeta(groups, Collections.emptyMap());

        Assert.assertEquals(withExtra1.generateHashCodeForBeaconCheck(true), withExtra2.generateHashCodeForBeaconCheck(true));
        Assert.assertNotEquals(withExtra1.generateHashCodeForBeaconCheck(true), withExtra3.generateHashCodeForBeaconCheck(true));
        Assert.assertNotEquals(withoutExtra.generateHashCodeForBeaconCheck(false), withExtra1.generateHashCodeForBeaconCheck(true));
        Assert.assertEquals(withNullExtra.generateHashCodeForBeaconCheck(false), withEmptyExtra.generateHashCodeForBeaconCheck(false));
        Assert.assertEquals(withoutExtra.generateHashCodeForBeaconCheck(false), withExtra1.generateHashCodeForBeaconCheck(false));
        Assert.assertEquals(withoutExtra.generateHashCodeForBeaconCheck(false), withEmptyExtra.generateHashCodeForBeaconCheck(false));
    }
}
