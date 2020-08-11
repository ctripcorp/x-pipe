package com.ctrip.xpipe.redis.console.healthcheck.actions.redismaster;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class DefaultRedisMasterActionListenerTest extends AbstractConsoleTest {

    private DefaultRedisMasterActionListener listener;

    @Mock
    private RedisService redisService;

    @Mock
    private MetaCache metaCache;

    @Mock
    private MetaServerConsoleServiceManagerWrapper metaServerConsoleServiceManagerWrapper;

    @Mock
    private MetaServerConsoleService metaServerConsoleService;

    private RedisHealthCheckInstance instance;

    private List<RedisTbl> redisTbls = new ArrayList<RedisTbl>(){{
        add(new RedisTbl().setRedisIp("127.0.0.1").setRedisPort(6379).setMaster(true));
        add(new RedisTbl().setRedisIp("127.0.0.1").setRedisPort(6479).setMaster(false));
        add(new RedisTbl().setRedisIp("127.0.0.1").setRedisPort(6579).setMaster(false));
    }};

    private List<RedisTbl> updatedRedisTbls;

    @Before
    public void setupDefaultRedisMasterActionListenerTest() throws Exception {
        listener = new DefaultRedisMasterActionListener(redisService, metaCache, metaServerConsoleServiceManagerWrapper);
        listener = Mockito.spy(listener);
        instance = newRandomRedisHealthCheckInstance(6379);

        Mockito.when(metaServerConsoleServiceManagerWrapper.getFastService(Mockito.anyString())).thenReturn(metaServerConsoleService);
        Mockito.doAnswer((invocation) -> mockXpipeMeta()).when(metaCache).getXpipeMeta();
        Mockito.doAnswer((invocation) -> redisTbls).when(redisService).findRedisesByDcClusterShard(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        Mockito.doAnswer(invocation -> {
            updatedRedisTbls = invocation.getArgumentAt(0, List.class);
            return null;
        }).when(redisService).updateBatchMaster(Mockito.anyList());
    }

    @Test
    public void testExpectMasterAndMaster() {
        instance.getRedisInstanceInfo().isMaster(true);
        listener.onAction(new RedisMasterActionContext(instance, Server.SERVER_ROLE.MASTER));
        Mockito.verify(listener, Mockito.never()).handleUnknownRole(Mockito.any());
        Mockito.verify(listener, Mockito.never()).updateRedisRoleInDB(Mockito.any(), Mockito.any());
    }

    @Test
    public void testExpectSlaveAndSlave() {
        instance.getRedisInstanceInfo().isMaster(false);
        listener.onAction(new RedisMasterActionContext(instance, Server.SERVER_ROLE.SLAVE));
        Mockito.verify(listener, Mockito.never()).handleUnknownRole(Mockito.any());
        Mockito.verify(listener, Mockito.never()).updateRedisRoleInDB(Mockito.any(), Mockito.any());
    }

    @Test
    public void testExpectMasterButSlave() {
        instance.getRedisInstanceInfo().isMaster(true);
        listener.onAction(new RedisMasterActionContext(instance, Server.SERVER_ROLE.SLAVE));
        Mockito.verify(listener, Mockito.never()).handleUnknownRole(Mockito.any());
        Mockito.verify(listener, Mockito.times(1)).updateRedisRoleInDB(Mockito.any(), Mockito.any());
        Assert.assertFalse(instance.getRedisInstanceInfo().isMaster());
        Assert.assertEquals(1, updatedRedisTbls.size());
        Assert.assertEquals(new RedisTbl().setRedisIp("127.0.0.1").setRedisPort(6379).setMaster(false).toString(), updatedRedisTbls.get(0).toString());
    }

    @Test
    public void testExpectSlaveButMaster() {
        instance.getRedisInstanceInfo().isMaster(false);
        redisTbls.get(0).setMaster(false);
        listener.onAction(new RedisMasterActionContext(instance, Server.SERVER_ROLE.MASTER));
        Mockito.verify(listener, Mockito.never()).handleUnknownRole(Mockito.any());
        Mockito.verify(listener, Mockito.times(1)).updateRedisRoleInDB(Mockito.any(), Mockito.any());
        Assert.assertTrue(instance.getRedisInstanceInfo().isMaster());
        Assert.assertEquals(1, updatedRedisTbls.size());
        Assert.assertEquals(new RedisTbl().setRedisIp("127.0.0.1").setRedisPort(6379).setMaster(true).toString(), updatedRedisTbls.get(0).toString());
    }

    @Test
    public void testMultiMaster() {
        redisTbls.get(1).setMaster(true);
        testExpectMasterAndMaster();

        redisTbls.get(2).setMaster(true);
        testExpectSlaveAndSlave();
    }

    @Test
    public void testBecomeMasterWithMultiMaster() {
        redisTbls.get(1).setMaster(true);
        redisTbls.get(2).setMaster(true);
        testExpectSlaveButMaster();
    }

    @Test
    public void testBecomeSlaveWithMultiMaster() {
        redisTbls.get(1).setMaster(true);
        redisTbls.get(2).setMaster(true);
        testExpectMasterButSlave();
    }

    @Test
    public void testUnknownAndSlave() {
        instance.getRedisInstanceInfo().isMaster(false);
        redisTbls.get(0).setMaster(false);
        listener.onAction(new RedisMasterActionContext(instance, Server.SERVER_ROLE.UNKNOWN));
        sleep(30);
        Mockito.verify(listener, Mockito.times(1)).handleUnknownRole(Mockito.any());
        Mockito.verify(listener, Mockito.never()).updateRedisRoleInDB(Mockito.any(), Mockito.any());
        Mockito.verify(metaCache, Mockito.never()).getXpipeMeta();
        Mockito.verify(metaServerConsoleServiceManagerWrapper, Mockito.never()).getFastService(Mockito.anyString());
    }

    @Test
    public void testUnknownAndMaster() {
        instance.getRedisInstanceInfo().isMaster(true);
        listener.onAction(new RedisMasterActionContext(instance, Server.SERVER_ROLE.UNKNOWN));
        sleep(30);
        Mockito.verify(listener, Mockito.never()).updateRedisRoleInDB(Mockito.any(), Mockito.any());
        Mockito.verify(metaServerConsoleServiceManagerWrapper, Mockito.never()).getFastService(Mockito.anyString());
    }

    @Test
    public void testUnknownAndMasterAndMetaServerTakeThis() {
        redisTbls.get(1).setMaster(true); // multi master
        instance.getRedisInstanceInfo().isMaster(true);
        Mockito.when(metaServerConsoleService.getCurrentMaster(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(new RedisMeta().setIp("127.0.0.1").setPort(6379));
        listener.onAction(new RedisMasterActionContext(instance, Server.SERVER_ROLE.UNKNOWN));
        sleep(30);
        Mockito.verify(listener, Mockito.never()).updateRedisRoleInDB(Mockito.any(), Mockito.any());
    }

    @Test
    public void testUnknownAndMasterButMetaServerTakeTheOther() {
        redisTbls.get(1).setMaster(true);
        instance.getRedisInstanceInfo().isMaster(true);
        Mockito.when(metaServerConsoleService.getCurrentMaster(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(new RedisMeta().setIp("127.0.0.1").setPort(6479));
        listener.onAction(new RedisMasterActionContext(instance, Server.SERVER_ROLE.UNKNOWN));
        sleep(30);
        Mockito.verify(redisService, Mockito.times(1)).updateBatchMaster(Mockito.anyList());
        Assert.assertFalse(instance.getRedisInstanceInfo().isMaster());
        Assert.assertEquals(new RedisTbl().setRedisIp("127.0.0.1").setRedisPort(6379).setMaster(false).toString(), updatedRedisTbls.get(0).toString());
    }

    @Test
    public void testUnknownAndMasterButMetaServerFail() {
        redisTbls.get(1).setMaster(true); // multi master
        instance.getRedisInstanceInfo().isMaster(true);
        Mockito.when(metaServerConsoleService.getCurrentMaster(Mockito.anyString(), Mockito.anyString()))
                .thenThrow(new RuntimeException());
        listener.onAction(new RedisMasterActionContext(instance, Server.SERVER_ROLE.UNKNOWN));
        sleep(30);
        Mockito.verify(listener, Mockito.never()).updateRedisRoleInDB(Mockito.any(), Mockito.any());
    }

    private XpipeMeta mockXpipeMeta() {
        RedisInstanceInfo info = instance.getRedisInstanceInfo();
        String dcId = info.getDcId();
        String clusterId = info.getClusterId();
        String shardId = info.getShardId();

        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta dcMeta = new DcMeta(dcId);
        ClusterMeta clusterMeta = new ClusterMeta(clusterId);
        ShardMeta shardMeta = new ShardMeta(shardId);
        redisTbls.forEach(redisTbl -> {
            if (redisTbl.isMaster()) {
                shardMeta.addRedis(new RedisMeta().setIp(redisTbl.getRedisIp()).setPort(redisTbl.getRedisPort()).setMaster(""));
            } else {
                shardMeta.addRedis(new RedisMeta().setIp(redisTbl.getRedisIp()).setPort(redisTbl.getRedisPort()).setMaster(XPipeConsoleConstant.DEFAULT_ADDRESS));
            }
        });

        return xpipeMeta.addDc(dcMeta.addCluster(clusterMeta.addShard(shardMeta)));
    }

}
