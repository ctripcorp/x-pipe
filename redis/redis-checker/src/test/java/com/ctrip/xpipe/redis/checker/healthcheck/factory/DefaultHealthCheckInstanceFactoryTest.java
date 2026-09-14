package com.ctrip.xpipe.redis.checker.healthcheck.factory;

import com.ctrip.framework.xpipe.redis.ProxyRegistry;
import com.ctrip.framework.xpipe.redis.proxy.ProxyResourceManager;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultHealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultHealthCheckInstanceFactory;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Sep 04, 2018
 */
public class DefaultHealthCheckInstanceFactoryTest extends AbstractCheckerIntegrationTest {

    @Autowired
    protected DefaultHealthCheckInstanceFactory factory;

    @Autowired
    private DefaultHealthCheckEndpointFactory endpointFactory;

    private MetaCache metaCache;
    
    private MetaCache oldMetaCache;

    @Before
    public void beforeDefaultHealthCheckRedisInstanceFactoryTest() {
        oldMetaCache = endpointFactory.getMetaCache();
        metaCache = mock(MetaCache.class);
        endpointFactory.setMetaCache(metaCache);
    }
    
    @After
    public void afterDefaultHealthCheckRedisInstanceFactoryTest() {
        endpointFactory.setMetaCache(oldMetaCache);
    }

    @Test
    public void testCreate() {
        RedisMeta redisMeta = normalRedisMeta();
        when(metaCache.getDc(new HostPort(redisMeta.getIp(), redisMeta.getPort()))).thenReturn("oy");
        RedisHealthCheckInstance instance = factory.create(redisMeta);

        Assert.assertNotNull(instance.getEndpoint());
        Assert.assertNotNull(instance.getHealthCheckConfig());
        Assert.assertNotNull(instance.getCheckInfo());
        Assert.assertNotNull(instance.getCheckInfo().getRedisCheckRules());
        Assert.assertNotNull(instance.getRedisSession());

        Assert.assertEquals(instance.getEndpoint(), new DefaultEndPoint(redisMeta.getIp(), redisMeta.getPort()));
        Assert.assertTrue(instance.getLifecycleState().isStarted());
        factory.remove(instance);
    }

    @Test
    public void testCreateKeeper() {
        KeeperMeta keeperMeta = normalKeeperMeta();
        KeeperHealthCheckInstance first = factory.create(keeperMeta);
        KeeperHealthCheckInstance second = factory.create(keeperMeta);

        Assert.assertEquals(new DefaultEndPoint(keeperMeta.getIp(), keeperMeta.getPort()), first.getEndpoint());
        Assert.assertEquals(first.getEndpoint(), second.getEndpoint());
        Assert.assertSame(first.getRedisSession(), second.getRedisSession());
        Assert.assertNotNull(first.getHealthCheckConfig());
        Assert.assertTrue(first.getHealthCheckActions().isEmpty());
        Assert.assertFalse(first.getLifecycleState().isStarted());

        KeeperInstanceInfo info = first.getCheckInfo();
        Assert.assertEquals("cluster", info.getClusterId());
        Assert.assertEquals(42, info.getClusterOrgId());
        Assert.assertEquals("shard", info.getShardId());
        Assert.assertEquals(Long.valueOf(100L), info.getShardDbId());
        Assert.assertEquals("jq", info.getDcId());
        Assert.assertEquals("oy", info.getActiveDc());
        Assert.assertEquals(ClusterType.ONE_WAY, info.getClusterType());
        Assert.assertEquals(new HostPort("127.0.0.1", 6380), info.getHostPort());
        Assert.assertEquals("normal", info.getStatus());
        Assert.assertFalse(RedisHealthCheckInstance.class.isAssignableFrom(first.getClass()));
        Assert.assertFalse(RedisInstanceInfo.class.isAssignableFrom(info.getClass()));
        Assert.assertFalse(Arrays.stream(KeeperInstanceInfo.class.getMethods())
                .anyMatch(method -> "getCreateTime".equals(method.getName())));

        factory.remove(first);
        factory.remove(second);
    }

    @Test
    public void testCreateRedisInstanceInfoWithCreateTime() {
        long createTimeMillis = System.currentTimeMillis();
        RedisMeta redisMeta = normalRedisMeta().setCreateTime(createTimeMillis);
        when(metaCache.getDc(new HostPort(redisMeta.getIp(), redisMeta.getPort()))).thenReturn("oy");

        RedisHealthCheckInstance instance = factory.create(redisMeta);

        Assert.assertEquals(new java.util.Date(createTimeMillis), instance.getCheckInfo().getCreateTime());
        factory.remove(instance);
    }

    @Test
    public void testKeeperRemovalDoesNotAffectRedisProxyAtSameAddress() {
        XpipeMeta meta = new XpipeMeta();
        DcMeta local = newDcMeta(FoundationService.DEFAULT.getDataCenter());
        meta.addDc(local);
        DcMeta target = newDcMeta("target");
        RedisMeta redisMeta = target.getClusters().get("cluster").getShards().get("shard").getRedises().get(0);
        meta.addDc(target);

        String routeInfo = "PROXYTCP://127.0.0.1:8008,PROXYTCP://127.0.0.1:8009";
        local.addRoute(new RouteMeta().setSrcDc(FoundationService.DEFAULT.getDataCenter())
                .setDstDc("target").setTag(Route.TAG_CONSOLE).setRouteInfo(routeInfo).setIsPublic(true)
                .setClusterType("").setOrgId(0));

        ClusterMeta clusterMeta = redisMeta.parent().parent();
        HostPort address = new HostPort(redisMeta.getIp(), redisMeta.getPort());
        when(metaCache.getCurrentDcConsoleRoutes()).thenReturn(local.getRoutes());
        when(metaCache.getXpipeMeta()).thenReturn(meta);
        when(metaCache.findMetaDesc(address))
                .thenReturn(new XpipeMetaManager.MetaDesc(clusterMeta.parent(), clusterMeta, redisMeta.parent(), redisMeta));
        when(metaCache.getDc(address)).thenReturn("target");

        endpointFactory.updateRoutes();
        RedisHealthCheckInstance redisInstance = factory.create(redisMeta);
        Assert.assertTrue(redisInstance.getEndpoint() instanceof DefaultEndPoint);
        Assert.assertEquals(AbstractRedisCommand.PROXYED_REDIS_CONNECTION_COMMAND_TIME_OUT_MILLI,
                redisInstance.getRedisSession().getCommandTimeOut());
        KeeperMeta keeperMeta = normalKeeperMeta().setIp(redisMeta.getIp()).setPort(redisMeta.getPort());
        KeeperHealthCheckInstance keeperInstance = null;
        try {
            ProxyResourceManager redisProxy = ProxyRegistry.getProxy(address.getHost(), address.getPort());
            Assert.assertNotNull(redisProxy);

            keeperInstance = factory.create(keeperMeta);
            Assert.assertSame(redisProxy, ProxyRegistry.getProxy(address.getHost(), address.getPort()));
            Assert.assertNotSame(redisInstance.getRedisSession(), keeperInstance.getRedisSession());

            factory.remove(keeperInstance);
            keeperInstance = null;
            Assert.assertSame(redisProxy, ProxyRegistry.getProxy(address.getHost(), address.getPort()));
            Assert.assertSame(redisInstance.getEndpoint(), endpointFactory.getOrCreateEndpoint(redisMeta));
        } finally {
            if (keeperInstance != null) {
                factory.remove(keeperInstance);
            }
            factory.remove(redisInstance);
        }
    }

    protected DcMeta newDcMeta(String dcId) {
        DcMeta dcMeta = new DcMeta().setId(dcId);
        ClusterMeta clusterMeta = new ClusterMeta().setId("cluster").setParent(dcMeta)
                .setType(ClusterType.ONE_WAY.toString()).setOrgId(0);
        dcMeta.addCluster(clusterMeta);
        ShardMeta shardMeta = new ShardMeta().setParent(clusterMeta).setId("shard");
        clusterMeta.addShard(shardMeta);
        RedisMeta redisMeta = new RedisMeta().setParent(shardMeta).setIp("localhost").setPort(randomPort());
        shardMeta.addRedis(redisMeta);
        return dcMeta;
    }

    protected RedisMeta normalRedisMeta() {
        DcMeta dcMeta = new DcMeta().setId("dc");
        ClusterMeta clusterMeta = new ClusterMeta().setId("cluster").setParent(dcMeta)
                .setType(ClusterType.ONE_WAY.toString()).setOrgId(0).setActiveRedisCheckRules("0,1");
        ShardMeta shardMeta = new ShardMeta().setParent(clusterMeta).setId("shard");
        return new RedisMeta().setParent(shardMeta).setIp("localhost").setPort(randomPort());
    }

    protected KeeperMeta normalKeeperMeta() {
        DcMeta dcMeta = new DcMeta().setId("jq");
        ClusterMeta clusterMeta = new ClusterMeta().setId("cluster").setType(ClusterType.ONE_WAY.toString())
                .setActiveDc("oy").setOrgId(42).setStatus("normal");
        dcMeta.addCluster(clusterMeta);
        ShardMeta shardMeta = new ShardMeta().setId("shard").setDbId(100L);
        clusterMeta.addShard(shardMeta);
        KeeperMeta keeperMeta = new KeeperMeta().setIp("127.0.0.1").setPort(6380)
                .setActive(true).setMaster("127.0.0.2:6379");
        shardMeta.addKeeper(keeperMeta);
        return keeperMeta;
    }
    
}
