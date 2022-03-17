package com.ctrip.xpipe.redis.checker.healthcheck.factory;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultHealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultHealthCheckInstanceFactory;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.mockito.Matchers.any;
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
    public void testCreateProxyEnabledInstance() {
        XpipeMeta meta = new XpipeMeta();
        DcMeta local = newDcMeta(FoundationService.DEFAULT.getDataCenter());
        meta.addDc(local);
        DcMeta target = newDcMeta("target");
        RedisMeta redisMeta = target.getClusters().get("cluster").getShards().get("shard").getRedises().get(0);
        meta.addDc(target);

        String routeInfo = "PROXYTCP://127.0.0.1:8008,PROXYTCP://127.0.0.1:8009";
        local.addRoute(new RouteMeta().setSrcDc(FoundationService.DEFAULT.getDataCenter())
                .setDstDc("target").setTag(Route.TAG_CONSOLE).setRouteInfo(routeInfo));

        when(metaCache.getRoutes()).thenReturn(local.getRoutes());
        when(metaCache.getXpipeMeta()).thenReturn(meta);

        logger.info("{}", metaCache.getXpipeMeta().toString());
        logger.info("{}", metaCache.getRoutes());

        when(metaCache.getDc(new HostPort(redisMeta.getIp(), redisMeta.getPort()))).thenReturn("target");
        endpointFactory.updateRoutes();
        RedisHealthCheckInstance instance = factory.create(redisMeta);

        Assert.assertTrue(instance.getEndpoint() instanceof DefaultEndPoint);
        Assert.assertEquals(AbstractRedisCommand.PROXYED_REDIS_CONNECTION_COMMAND_TIME_OUT_MILLI,
                instance.getRedisSession().getCommandTimeOut());
        factory.remove(instance);
    }

    protected DcMeta newDcMeta(String dcId) {
        DcMeta dcMeta = new DcMeta().setId(dcId);
        ClusterMeta clusterMeta = new ClusterMeta().setId("cluster").setParent(dcMeta).setType(ClusterType.ONE_WAY.toString());
        dcMeta.addCluster(clusterMeta);
        ShardMeta shardMeta = new ShardMeta().setParent(clusterMeta).setId("shard");
        clusterMeta.addShard(shardMeta);
        RedisMeta redisMeta = new RedisMeta().setParent(shardMeta).setIp("localhost").setPort(randomPort());
        shardMeta.addRedis(redisMeta);
        return dcMeta;
    }

    protected RedisMeta normalRedisMeta() {
        DcMeta dcMeta = new DcMeta().setId("dc");
        ClusterMeta clusterMeta = new ClusterMeta().setId("cluster").setParent(dcMeta).setType(ClusterType.ONE_WAY.toString()).setActiveRedisCheckRules("0,1");
        ShardMeta shardMeta = new ShardMeta().setParent(clusterMeta).setId("shard");
        RedisMeta redisMeta = new RedisMeta().setParent(shardMeta).setIp("localhost").setPort(randomPort());
        return redisMeta;
    }
    
    
}