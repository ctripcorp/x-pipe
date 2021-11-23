package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.MetaServerManager;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.anyString;

@RunWith(MockitoJUnitRunner.class)
public class DefaultRedisMasterActionListenerTest extends AbstractCheckerTest {

    private DefaultRedisMasterActionListener listener;

    @Mock
    private PersistenceCache persistenceCache;

    @Mock
    private MetaCache metaCache;

    @Mock
    private MetaServerManager metaServerManager;

    private RedisHealthCheckInstance instance;

//    private List<RedisTbl> redisTbls = new ArrayList<RedisTbl>(){{
//        add(new RedisTbl().setRedisIp("127.0.0.1").setRedisPort(6379).setMaster(true));
//        add(new RedisTbl().setRedisIp("127.0.0.1").setRedisPort(6479).setMaster(false));
//        add(new RedisTbl().setRedisIp("127.0.0.1").setRedisPort(6579).setMaster(false));
//    }};
//
//    private List<RedisTbl> updatedRedisTbls;

    private List<Pair<HostPort, Boolean>> redises = new ArrayList<Pair<HostPort, Boolean>>() {{
        add(new Pair<>(new HostPort("127.0.0.1", 6379), true));
        add(new Pair<>(new HostPort("127.0.0.1", 6479), false));
        add(new Pair<>(new HostPort("127.0.0.1", 6579), false));
    }};

    private HostPort updateRedis;

    private Server.SERVER_ROLE updateRole;

    @Before
    public void setupDefaultRedisMasterActionListenerTest() throws Exception {
        listener = new DefaultRedisMasterActionListener(persistenceCache, metaCache, metaServerManager);
        listener = Mockito.spy(listener);
        instance = newRandomRedisHealthCheckInstance(6379);

//        Mockito.when(metaServerConsoleServiceManagerWrapper.getFastService(Mockito.anyString())).thenReturn(metaServerConsoleService);


        Mockito.doAnswer((invocation) -> mockXpipeMeta()).when(metaCache).getXpipeMeta();
//        Mockito.doAnswer((invocation) -> redisTbls).when(redisService).findRedisesByDcClusterShard(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
//        Mockito.doAnswer(invocation -> {
//            updatedRedisTbls = invocation.getArgumentAt(0, List.class);
//            return null;
//        }).when(redisService).updateBatchMaster(Mockito.anyList());
        Mockito.doAnswer(invocation -> {
            RedisHealthCheckInstance instance = invocation.getArgumentAt(0, RedisHealthCheckInstance.class);
            this.updateRole = invocation.getArgumentAt(1, Server.SERVER_ROLE.class);
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
    public void testUnknownAndSlave() {
        instance.getCheckInfo().isMaster(false);
        redises.get(0).setValue(false);
        listener.onAction(new RedisMasterActionContext(instance, new Exception("role fail")));
        sleep(30);
        Mockito.verify(listener, Mockito.times(1)).handleUnknownRole(Mockito.any());
        Mockito.verify(persistenceCache, Mockito.never()).updateRedisRole(Mockito.any(), Mockito.any());
        Mockito.verify(metaCache, Mockito.never()).getXpipeMeta();
        Mockito.verify(metaServerManager, Mockito.never()).getCurrentMaster(anyString(), anyString(), anyString());
    }

    @Test
    public void testUnknownAndMaster() {
        instance.getCheckInfo().isMaster(true);
        listener.onAction(new RedisMasterActionContext(instance, new Exception("role fail")));
        sleep(30);
        Mockito.verify(persistenceCache, Mockito.never()).updateRedisRole(Mockito.any(), Mockito.any());
        Mockito.verify(metaServerManager, Mockito.never()).getCurrentMaster(anyString(), anyString(), anyString());
    }

    @Test
    public void testUnknownAndMasterAndMetaServerTakeThis() {
        redises.get(1).setValue(true); // multi master
        instance.getCheckInfo().isMaster(true);
        Mockito.when(metaServerManager.getCurrentMaster(anyString(), anyString(), anyString()))
                .thenReturn(new RedisMeta().setIp("127.0.0.1").setPort(6379));
        listener.onAction(new RedisMasterActionContext(instance, new Exception("role fail")));
        sleep(30);
        Mockito.verify(persistenceCache, Mockito.never()).updateRedisRole(Mockito.any(), Mockito.any());
    }

    @Test
    public void testUnknownAndMasterButMetaServerTakeTheOther() {
        redises.get(1).setValue(true);
        instance.getCheckInfo().isMaster(true);
        Mockito.when(metaServerManager.getCurrentMaster(anyString(), anyString(), anyString()))
                .thenReturn(new RedisMeta().setIp("127.0.0.1").setPort(6479));
        listener.onAction(new RedisMasterActionContext(instance, new Exception("role fail")));
        sleep(30);
        Mockito.verify(persistenceCache, Mockito.times(1)).updateRedisRole(Mockito.any(), Mockito.any());
        Assert.assertEquals(new HostPort("127.0.0.1", 6379), updateRedis);
        Assert.assertEquals(Server.SERVER_ROLE.SLAVE, updateRole);
    }

    @Test
    public void testUnknownAndMasterButMetaServerFail() {
        redises.get(1).setValue(true); // multi master
        instance.getCheckInfo().isMaster(true);
        Mockito.when(metaServerManager.getCurrentMaster(anyString(), anyString(), anyString()))
                .thenThrow(new RuntimeException());
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
                shardMeta.addRedis(new RedisMeta().setIp(redis.getKey().getHost()).setPort(redis.getKey().getPort()).setMaster(""));
            } else {
                shardMeta.addRedis(new RedisMeta().setIp(redis.getKey().getHost()).setPort(redis.getKey().getPort()).setMaster("0.0.0.0:0"));
            }
        });

        return xpipeMeta.addDc(dcMeta.addCluster(clusterMeta.addShard(shardMeta)));
    }

}
