package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class OutClientRedisMasterActionListenerTest extends AbstractCheckerTest {

    private OuterClientRedisMasterActionListener listener;

    @Mock
    private PersistenceCache persistenceCache;

    @Mock
    private MetaCache metaCache;

    @Mock
    private OuterClientService outerClientService;

    private RedisHealthCheckInstance instance;

    private List<Pair<HostPort, Boolean>> redises = new ArrayList<Pair<HostPort, Boolean>>() {
        {
            add(new Pair<>(new HostPort("127.0.0.1", 6379), true));
            add(new Pair<>(new HostPort("127.0.0.1", 6479), false));
            add(new Pair<>(new HostPort("127.0.0.1", 6579), false));
        }
    };

    private HostPort updateRedis;

    private Server.SERVER_ROLE updateRole;

    @Before
    public void setupDefaultRedisMasterActionListenerTest() throws Exception {
        listener = new OuterClientRedisMasterActionListener(persistenceCache, metaCache);
        listener = Mockito.spy(listener);
        listener.setOuterClientService(outerClientService);
        instance = newRandomRedisHealthCheckInstance("dc", ClusterType.SINGLE_DC, 6379);

        Mockito.doAnswer((invocation) -> mockXpipeMeta()).when(metaCache).getXpipeMeta();
        Mockito.doAnswer(invocation -> {
            RedisHealthCheckInstance instance = invocation.getArgument(0, RedisHealthCheckInstance.class);
            this.updateRole = invocation.getArgument(1, Server.SERVER_ROLE.class);
            this.updateRedis = instance.getCheckInfo().getHostPort();
            return null;
        }).when(persistenceCache).updateRedisRole(Mockito.any(), Mockito.any());
    }

    @Test
    public void testExpectMasterAndMaster() {
        instance.getCheckInfo().isMaster(true);
        listener.onAction(new RedisMasterActionContext(instance, new TestRole(Server.SERVER_ROLE.MASTER)));
        Mockito.verify(listener, Mockito.never()).handleUnknownRole(Mockito.any());
        Mockito.verify(persistenceCache, Mockito.never()).updateRedisRole(Mockito.any(), Mockito.any());
    }

    @Test
    public void testExpectSlaveAndSlave() {
        instance.getCheckInfo().isMaster(false);
        listener.onAction(new RedisMasterActionContext(instance, new TestRole(Server.SERVER_ROLE.SLAVE)));
        Mockito.verify(listener, Mockito.never()).handleUnknownRole(Mockito.any());
        Mockito.verify(persistenceCache, Mockito.never()).updateRedisRole(Mockito.any(), Mockito.any());
    }

    @Test
    public void testExpectMasterButSlave() {
        instance.getCheckInfo().isMaster(true);
        listener.onAction(new RedisMasterActionContext(instance, new TestRole(Server.SERVER_ROLE.SLAVE)));
        Mockito.verify(listener, Mockito.never()).handleUnknownRole(Mockito.any());
        Mockito.verify(persistenceCache, Mockito.times(1)).updateRedisRole(Mockito.any(), Mockito.any());
        Assert.assertEquals(new HostPort("127.0.0.1", 6379), updateRedis);
        Assert.assertEquals(Server.SERVER_ROLE.SLAVE, updateRole);
    }

    @Test
    public void testExpectSlaveButMaster() {
        instance.getCheckInfo().isMaster(false);
        listener.onAction(new RedisMasterActionContext(instance, new TestRole(Server.SERVER_ROLE.MASTER)));
        Mockito.verify(listener, Mockito.never()).handleUnknownRole(Mockito.any());
        Mockito.verify(persistenceCache, Mockito.times(1)).updateRedisRole(Mockito.any(), Mockito.any());
        Assert.assertEquals(new HostPort("127.0.0.1", 6379), updateRedis);
        Assert.assertEquals(Server.SERVER_ROLE.MASTER, updateRole);
    }

    @Test
    public void testMultiMaster() {
        redises.get(1).setValue(true);
        testExpectMasterAndMaster();

        redises.get(2).setValue(true);
        testExpectSlaveAndSlave();
    }

    @Test
    public void testBecomeMasterWithMultiMaster() {
        redises.get(1).setValue(true);
        redises.get(2).setValue(true);
        testExpectSlaveButMaster();
    }

    @Test
    public void testBecomeSlaveWithMultiMaster() {
        redises.get(1).setValue(true);
        redises.get(2).setValue(true);
        testExpectMasterButSlave();
    }

    @Test
    public void testUnknownAndSlave() throws Exception {
        instance.getCheckInfo().isMaster(false);
        redises.get(0).setValue(false);
        listener.onAction(new RedisMasterActionContext(instance, new Exception("role fail")));
        sleep(30);
        Mockito.verify(listener, Mockito.times(1)).handleUnknownRole(Mockito.any());
        Mockito.verify(persistenceCache, Mockito.never()).updateRedisRole(Mockito.any(), Mockito.any());
        Mockito.verify(metaCache, Mockito.never()).getXpipeMeta();
        Mockito.verify(outerClientService, Mockito.never()).getClusterInfo(anyString());
    }

    @Test
    public void testUnknownAndMaster() throws Exception {
        instance.getCheckInfo().isMaster(true);
        listener.onAction(new RedisMasterActionContext(instance, new Exception("role fail")));
        sleep(30);
        Mockito.verify(persistenceCache, Mockito.never()).updateRedisRole(Mockito.any(), Mockito.any());
        Mockito.verify(outerClientService, Mockito.never()).getClusterInfo(anyString());
    }

    @Test
    public void testUnknownAndMasterAndOutClientTakeThis() throws Exception {
        redises.get(1).setValue(true); // multi master
        instance.getCheckInfo().isMaster(true);
        doReturn(new RedisMeta().setIp("127.0.0.1").setPort(6379)).when(listener).finalMaster(anyString(), anyString(),
                anyString());
        listener.onAction(new RedisMasterActionContext(instance, new Exception("role fail")));
        sleep(30);
        Mockito.verify(persistenceCache, Mockito.never()).updateRedisRole(Mockito.any(), Mockito.any());
    }

    @Test
    public void testUnknownAndMasterButOutClientTakeTheOther() {
        redises.get(1).setValue(true);
        instance.getCheckInfo().isMaster(true);
        doReturn(new RedisMeta().setIp("127.0.0.1").setPort(6479)).when(listener).finalMaster(anyString(), anyString(),
                anyString());
        listener.onAction(new RedisMasterActionContext(instance, new Exception("role fail")));
        sleep(30);
        Mockito.verify(persistenceCache, Mockito.times(1)).updateRedisRole(Mockito.any(), Mockito.any());
        Assert.assertEquals(new HostPort("127.0.0.1", 6379), updateRedis);
        Assert.assertEquals(Server.SERVER_ROLE.SLAVE, updateRole);
    }

    @Test
    public void testUnknownAndMasterButOutClientFail() {
        redises.get(1).setValue(true); // multi master
        instance.getCheckInfo().isMaster(true);
        doThrow(new RuntimeException()).when(listener).finalMaster(anyString(), anyString(), anyString());
        listener.onAction(new RedisMasterActionContext(instance, new Exception("role fail")));
        sleep(30);
        Mockito.verify(persistenceCache, Mockito.never()).updateRedisRole(Mockito.any(), Mockito.any());
    }

    private XpipeMeta mockXpipeMeta() {
        RedisInstanceInfo info = instance.getCheckInfo();
        String dcId = info.getDcId();
        String clusterId = info.getClusterId();
        String shardId = info.getShardId();

        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta dcMeta = new DcMeta(dcId);
        ClusterMeta clusterMeta = new ClusterMeta(clusterId);
        ShardMeta shardMeta = new ShardMeta(shardId);
        redises.forEach(redis -> {
            if (redis.getValue()) {
                shardMeta.addRedis(new RedisMeta().setIp(redis.getKey().getHost()).setPort(redis.getKey().getPort())
                        .setMaster(""));
            } else {
                shardMeta.addRedis(new RedisMeta().setIp(redis.getKey().getHost()).setPort(redis.getKey().getPort())
                        .setMaster("0.0.0.0:0"));
            }
        });

        return xpipeMeta.addDc(dcMeta.addCluster(clusterMeta.addShard(shardMeta)));
    }

    @Test
    public void finalMasterSuccessTest() throws Exception {
        when(outerClientService.getClusterInfo(anyString())).thenReturn(
                new OuterClientService.ClusterInfo().setGroups(Lists.newArrayList(
                        new OuterClientService.GroupInfo().setName("shard1").setInstances(Lists.newArrayList(
                                new OuterClientService.InstanceInfo().setIsMaster(true).setIPAddress(LOCAL_HOST)
                                        .setPort(6379).setEnv("dc1").setStatus(true),
                                new OuterClientService.InstanceInfo().setIsMaster(true).setIPAddress(LOCAL_HOST)
                                        .setPort(6380).setEnv("dc1").setStatus(false),
                                new OuterClientService.InstanceInfo().setIsMaster(false).setIPAddress(LOCAL_HOST)
                                        .setPort(6381).setEnv("dc1").setStatus(true),
                                new OuterClientService.InstanceInfo().setIsMaster(true).setIPAddress(LOCAL_HOST)
                                        .setPort(6382).setEnv("dc2").setStatus(true),
                                new OuterClientService.InstanceInfo().setIsMaster(false).setIPAddress(LOCAL_HOST)
                                        .setPort(6383).setEnv("dc2").setStatus(true))),
                        new OuterClientService.GroupInfo().setName("shard2").setInstances(Lists.newArrayList(
                                new OuterClientService.InstanceInfo().setIsMaster(true).setIPAddress(LOCAL_HOST)
                                        .setPort(6384).setEnv("dc1").setStatus(true),
                                new OuterClientService.InstanceInfo().setIsMaster(false).setIPAddress(LOCAL_HOST)
                                        .setPort(6386).setEnv("dc1").setStatus(true),
                                new OuterClientService.InstanceInfo().setIsMaster(true).setIPAddress(LOCAL_HOST)
                                        .setPort(6387).setEnv("dc2").setStatus(true),
                                new OuterClientService.InstanceInfo().setIsMaster(false).setIPAddress(LOCAL_HOST)
                                        .setPort(6388).setEnv("dc2").setStatus(true))))));

        RedisMeta finalMaster = listener.finalMaster("dc1", "cluster1", "shard1");
        Assert.assertEquals(6379, finalMaster.getPort().intValue());
    }

    @Test
    public void finalMasterNotFoundTest() throws Exception {
        when(outerClientService.getClusterInfo(anyString())).thenReturn(
                new OuterClientService.ClusterInfo().setGroups(Lists.newArrayList(
                        new OuterClientService.GroupInfo().setName("shard1").setInstances(Lists.newArrayList(
                                new OuterClientService.InstanceInfo().setIsMaster(true).setIPAddress(LOCAL_HOST)
                                        .setPort(6379).setEnv("dc1").setStatus(false),
                                new OuterClientService.InstanceInfo().setIsMaster(true).setIPAddress(LOCAL_HOST)
                                        .setPort(6380).setEnv("dc1").setStatus(false),
                                new OuterClientService.InstanceInfo().setIsMaster(false).setIPAddress(LOCAL_HOST)
                                        .setPort(6381).setEnv("dc1").setStatus(true),
                                new OuterClientService.InstanceInfo().setIsMaster(true).setIPAddress(LOCAL_HOST)
                                        .setPort(6382).setEnv("dc2").setStatus(true),
                                new OuterClientService.InstanceInfo().setIsMaster(false).setIPAddress(LOCAL_HOST)
                                        .setPort(6383).setEnv("dc2").setStatus(true))),
                        new OuterClientService.GroupInfo().setName("shard2").setInstances(Lists.newArrayList(
                                new OuterClientService.InstanceInfo().setIsMaster(true).setIPAddress(LOCAL_HOST)
                                        .setPort(6384).setEnv("dc1").setStatus(true),
                                new OuterClientService.InstanceInfo().setIsMaster(false).setIPAddress(LOCAL_HOST)
                                        .setPort(6386).setEnv("dc1").setStatus(true),
                                new OuterClientService.InstanceInfo().setIsMaster(true).setIPAddress(LOCAL_HOST)
                                        .setPort(6387).setEnv("dc2").setStatus(true),
                                new OuterClientService.InstanceInfo().setIsMaster(false).setIPAddress(LOCAL_HOST)
                                        .setPort(6388).setEnv("dc2").setStatus(true))))));

        try {
            listener.finalMaster("dc1", "cluster1", "shard1");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("no active master"));
        }

    }

    @Test
    public void finalMasterTooManyTest() throws Exception {
        when(outerClientService.getClusterInfo(anyString())).thenReturn(
                new OuterClientService.ClusterInfo().setGroups(Lists.newArrayList(
                        new OuterClientService.GroupInfo().setName("shard1").setInstances(Lists.newArrayList(
                                new OuterClientService.InstanceInfo().setIsMaster(true).setIPAddress(LOCAL_HOST)
                                        .setPort(6379).setEnv("dc1").setStatus(true),
                                new OuterClientService.InstanceInfo().setIsMaster(true).setIPAddress(LOCAL_HOST)
                                        .setPort(6380).setEnv("dc1").setStatus(true),
                                new OuterClientService.InstanceInfo().setIsMaster(false).setIPAddress(LOCAL_HOST)
                                        .setPort(6381).setEnv("dc1").setStatus(true),
                                new OuterClientService.InstanceInfo().setIsMaster(true).setIPAddress(LOCAL_HOST)
                                        .setPort(6382).setEnv("dc2").setStatus(true),
                                new OuterClientService.InstanceInfo().setIsMaster(false).setIPAddress(LOCAL_HOST)
                                        .setPort(6383).setEnv("dc2").setStatus(true))),
                        new OuterClientService.GroupInfo().setName("shard2").setInstances(Lists.newArrayList(
                                new OuterClientService.InstanceInfo().setIsMaster(true).setIPAddress(LOCAL_HOST)
                                        .setPort(6384).setEnv("dc1").setStatus(true),
                                new OuterClientService.InstanceInfo().setIsMaster(false).setIPAddress(LOCAL_HOST)
                                        .setPort(6386).setEnv("dc1").setStatus(true),
                                new OuterClientService.InstanceInfo().setIsMaster(true).setIPAddress(LOCAL_HOST)
                                        .setPort(6387).setEnv("dc2").setStatus(true),
                                new OuterClientService.InstanceInfo().setIsMaster(false).setIPAddress(LOCAL_HOST)
                                        .setPort(6388).setEnv("dc2").setStatus(true))))));

        try {
            listener.finalMaster("dc1", "cluster1", "shard1");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("too many"));
        }

    }

}
