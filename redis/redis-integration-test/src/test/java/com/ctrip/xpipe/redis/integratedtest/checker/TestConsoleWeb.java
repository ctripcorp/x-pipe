package com.ctrip.xpipe.redis.integratedtest.checker;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.healthcheck.config.DefaultHealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.checker.resource.DefaultCheckerConsoleService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ZkServerMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterRole;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.RedisStartCmd;
import com.ctrip.xpipe.redis.integratedtest.metaserver.AbstractXpipeServerMultiDcTest;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition.KEY_SERVER_MODE;
import static com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition.SERVER_MODE.CONSOLE;
import static com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig.KEY_CLUSTER_SHARD_FOR_MIGRATE_SYS_CHECK;

public class TestConsoleWeb extends AbstractXpipeServerMultiDcTest {
    @Before
    public void start() throws Exception {
        startDb();    
    }
    
    @Test
    public void testPersistenceCache() throws Exception {
        XpipeNettyClientKeyedObjectPool pool = getXpipeNettyClientKeyedObjectPool();
        final int consolePort = 18080;
        final String consoleUrl = "http://127.0.0.1:" + 18080;

        Map<String, String> consoles = new HashMap<>();
        consoles.put("jq", "http://127.0.0.1:" + 1);
        consoles.put("fra", "http://127.0.0.1:" + consolePort);
        Map<String, String> metaServers = new HashMap<>();
        Map<String, String> extraOptions = new HashMap<>();
        extraOptions.put(KEY_CLUSTER_SHARD_FOR_MIGRATE_SYS_CHECK, "cluster-dr,cluster-dr-shard1");
        extraOptions.put(KEY_SERVER_MODE, CONSOLE.name());
        extraOptions.put("console.cluster.types", "one_way,bi_direction,ONE_WAY,BI_DIRECTION");

        ZkServerMeta jqZk = getZk(JQ_IDC);
        startSpringConsole(consolePort, JQ_IDC, jqZk.getAddress(), Collections.singletonList("127.0.0.1:" + consolePort), consoles, metaServers, extraOptions);

        CheckerConsoleService service = new DefaultCheckerConsoleService();

        
        Assert.assertEquals(0, service.clusterAlertWhiteList(consoleUrl).size());
        Assert.assertEquals(0, service.getProxyTunnelInfos(consoleUrl).size());
        Assert.assertEquals(1, service.loadAllClusterCreateTime(consoleUrl).size());
        Assert.assertEquals(true, service.isSentinelAutoProcess(consoleUrl));
        Assert.assertEquals(0, service.sentinelCheckWhiteList(consoleUrl).size());
        Assert.assertNotNull(service.getClusterCreateTime(consoleUrl, "cluster1"));
        Assert.assertEquals(service.isAlertSystemOn(consoleUrl), true);
        Assert.assertEquals(service.isClusterOnMigration(consoleUrl, "cluster1"), false);


        RedisMeta redisMeta = newRandomFakeRedisMeta().setPort(1000);
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(((ClusterMeta) redisMeta.parent().parent()).parent().getId(),
                ((ClusterMeta) redisMeta.parent().parent()).getId(), redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                redisMeta.parent().getActiveDc(), ClusterType.BI_DIRECTION);
        DefaultRedisHealthCheckInstance instance = new DefaultRedisHealthCheckInstance();
        instance.setInstanceInfo(info);
        instance.setEndpoint(new DefaultEndPoint(info.getHostPort().getHost(), info.getHostPort().getPort()));
        instance.setHealthCheckConfig(new DefaultHealthCheckConfig(buildCheckerConfig()));
        instance.setSession(new RedisSession(instance.getEndpoint(), scheduled, pool, buildCheckerConfig()));
        service.updateRedisRole( consoleUrl, instance, Server.SERVER_ROLE.MASTER);

        AlertMessageEntity alertMessageEntity = new AlertMessageEntity("Test", "test", Lists.newArrayList("test-list"), new AlertEntity(new HostPort("127.0.0.1", 6379), "jq", "cluster1", "shard1", "aaa", ALERT_TYPE.CRDT_CROSS_DC_REPLICATION_DOWN));
        service.recordAlert(consoleUrl, "127.0.0.3", alertMessageEntity,  () -> {
            Properties properties = new Properties();
            properties.setProperty("h", "t");
            return properties;
        });
    }

    @Test
    public void testChangeMaster() throws Exception {
        int consolePort = 18080;
        int checkerPort = 18000;

        XpipeNettyClientKeyedObjectPool pool = getXpipeNettyClientKeyedObjectPool();
        
        RedisStartCmd master = startCrdtRedis(1, 36380);
        RedisStartCmd slave = startCrdtRedis(1, 36379);
        waitConditionUntilTimeOut(() -> {
            try {
                new SlaveOfCommand(pool.getKeyPool(new DefaultEndPoint("127.0.0.1", 36379)), "127.0.0.1", 36380, scheduled).execute().get();
                return true;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            return false;
        }, 1000);
        
        waitConditionUntilTimeOut( () -> {
            try {
                return new RoleCommand(pool.getKeyPool(new DefaultEndPoint("127.0.0.1", 36379)), scheduled).execute().get() instanceof SlaveRole;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            return false;
        }, 5000, 1000);
        waitConditionUntilTimeOut( () -> {
            try {
                return new RoleCommand(pool.getKeyPool(new DefaultEndPoint("127.0.0.1", 36380)), scheduled).execute().get() instanceof MasterRole;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            };
            return false;
        }, 5000, 1000);


        ZkServerMeta jqZk = getZk(JQ_IDC);
        startZk(jqZk);

        Map<String, String> consoles = new HashMap<>();
        consoles.put("jq", "http://127.0.0.1:" + consolePort);
        consoles.put("fra", "http://127.0.0.1:" + consolePort);
        Map<String, String> metaServers = new HashMap<>();
        Map<String, String> extraOptions = new HashMap<>();
        extraOptions.put(KEY_CLUSTER_SHARD_FOR_MIGRATE_SYS_CHECK, "cluster-dr,cluster-dr-shard1");
        extraOptions.put(KEY_SERVER_MODE, CONSOLE.name());
        extraOptions.put("console.cluster.types", "one_way,bi_direction,ONE_WAY,BI_DIRECTION");


        startSpringConsole(consolePort, JQ_IDC, jqZk.getAddress(), Collections.singletonList("127.0.0.1:" + consolePort), consoles, metaServers, extraOptions);


        startSpringChecker(checkerPort, JQ_IDC, jqZk.getAddress(), Collections.singletonList("127.0.0.1:" + consolePort), "127.0.0.2");

        
        waitConditionUntilTimeOut(
                () -> {
                    List<Map<String, Object>> result = restTemplate.getForObject("http://127.0.0.1:" + consolePort + "/console/clusters/cluster1/dcs/jq/shards", List.class);
                    List<Map<String, Object>> redises = (List<Map<String, Object>>)result.get(0).get("redises");
                    for(Map<String, Object> redis: redises) {
                        logger.info("port: {}, master: {}", redis.get("redisPort"), redis.get("master"));
                        if(redis.get("redisPort").equals(36380) && redis.get("master").equals(true)) {
                            return true;
                        }
                    }
                    return false;
                }, 500000, 1000
        );
    }
    
    
    @After
    public void stop() throws Exception {
        stopAllServer();
    }
}
