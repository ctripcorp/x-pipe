package com.ctrip.xpipe.api.migration;

import com.ctrip.xpipe.api.migration.auto.data.MonitorClusterMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.endpoint.HostPort;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

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

        Assert.assertEquals(monitorClusterMeta1.generateHashCodeForBeaconCheck(), monitorClusterMeta2.generateHashCodeForBeaconCheck());
        Assert.assertNotEquals(monitorClusterMeta1.generateHashCodeForBeaconCheck(), monitorClusterMeta3.generateHashCodeForBeaconCheck());
        Assert.assertNotEquals(monitorClusterMeta1.generateHashCodeForBeaconCheck(), monitorClusterMeta4.generateHashCodeForBeaconCheck());
        Assert.assertNotEquals(monitorClusterMeta4.generateHashCodeForBeaconCheck(), monitorClusterMeta5.generateHashCodeForBeaconCheck());
        Assert.assertNotEquals(monitorClusterMeta5.generateHashCodeForBeaconCheck(), monitorClusterMeta6.generateHashCodeForBeaconCheck());
        System.out.println(monitorClusterMeta1.generateHashCodeForBeaconCheck());
        System.out.println(monitorClusterMeta2.generateHashCodeForBeaconCheck());
        System.out.println(monitorClusterMeta3.generateHashCodeForBeaconCheck());
        System.out.println(monitorClusterMeta4.generateHashCodeForBeaconCheck());
        System.out.println(monitorClusterMeta5.generateHashCodeForBeaconCheck());
        System.out.println(monitorClusterMeta6.generateHashCodeForBeaconCheck());
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
}
