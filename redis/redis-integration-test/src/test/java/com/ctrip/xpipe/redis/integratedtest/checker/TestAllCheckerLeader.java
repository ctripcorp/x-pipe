package com.ctrip.xpipe.redis.integratedtest.checker;


import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.healthcheck.config.DefaultHealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.checker.resource.DefaultCheckerConsoleService;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.core.entity.ZkServerMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractSentinelCommand;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.RedisStartCmd;
import com.ctrip.xpipe.redis.integratedtest.metaserver.AbstractXpipeServerMultiDcTest;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

import static com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition.KEY_SERVER_MODE;
import static com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition.SERVER_MODE.*;
import static com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig.KEY_CLUSTER_SHARD_FOR_MIGRATE_SYS_CHECK;

public class TestAllCheckerLeader extends AbstractXpipeServerMultiDcTest {
    
    public String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/xpipe-crdt-no-cluster.sql");
    }
    public Map<String, ConsoleInfo> defaultConsoleInfo() {
        Map<String, ConsoleInfo> consoleInfos = new HashMap<>();
        //start console + checker 2server
        consoleInfos.put("jq", new ConsoleInfo(CONSOLE).setConsole_port(18080).setChecker_port(28080));
        //start conset_checker 1server
        consoleInfos.put("oy", new ConsoleInfo(CONSOLE_CHECKER).setConsole_port(18081).setChecker_port(28081));
        //start checker 1 server
        consoleInfos.put("fra", new ConsoleInfo(CHECKER).setConsole_port(18080).setChecker_port(28082));
        return consoleInfos;
    }
    
    XpipeNettyClientKeyedObjectPool pool;
    
    @Before
    public void testBefore() throws Exception {
        startDb();
        pool = getXpipeNettyClientKeyedObjectPool();
    }

    final String sentinelMaster = "will-remove-master-name";
    final String localHost = "127.0.0.1";
    final int localPort = 6379;
    final int waitTime = 2000;
    @Test
    public void TestCheckerCleanSentinel() throws Exception {
        
        ZkServerMeta jqZk = getZk(JQ_IDC);
        startZk(jqZk);

        ZkServerMeta fraZk = getZk(FRA_IDC);
        startZk(fraZk);

        Map<Long, SentinelMeta> sentinel_metas = getXpipeMeta().getDcs().get(FRA_IDC).getSentinels();
        sentinel_metas.entrySet().stream().forEach(sentinel_meta -> {
            List<RedisStartCmd> sentinels = startSentinels(sentinel_meta.getValue());
            for(RedisStartCmd sentinel: sentinels) {
                try {
                    waitConditionUntilTimeOut(() -> sentinel.isProcessAlive(), 1000);
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
            }
        });
        
        
        int JQConsolePort = 18080;
        int FRACheckerPort = 18082;
        Map<String, String> consoles = new HashMap<>();
        consoles.put("jq", "http://127.0.0.1:" + JQConsolePort);
        consoles.put("fra", "http://127.0.0.1:" + JQConsolePort);
        Map<String, String> metaServers = new HashMap<>();
        Map<String, String> extraOptions = new HashMap<>();
        extraOptions.put(KEY_CLUSTER_SHARD_FOR_MIGRATE_SYS_CHECK, "cluster-dr,cluster-dr-shard1");
        extraOptions.put(KEY_SERVER_MODE, CONSOLE.name());
        extraOptions.put("console.cluster.types", "one_way,bi_direction,ONE_WAY,BI_DIRECTION");
        logger.info("========== start jq console ============");
        startSpringConsole(JQConsolePort, JQ_IDC, jqZk.getAddress(), Collections.singletonList("127.0.0.1:" + JQConsolePort), consoles, metaServers, extraOptions);
        
        logger.info("========== start fra checker ============");
        ConfigurableApplicationContext checker = startSpringChecker(FRACheckerPort, FRA_IDC, fraZk.getAddress(), Collections.singletonList("127.0.0.1:" + JQConsolePort), "127.0.0.3");

        waitConditionUntilTimeOut(() -> {
            Map<String, Object> healthInfo = restOperations.getForObject(String.format("http://%s:%d/health", localHost, JQConsolePort), Map.class);
            return (boolean)healthInfo.get("isLeader");
        }, 12000);
        
        
        waitConditionUntilTimeOut(() -> {
            return restOperations.getForObject(String.format("http://%s:%d/health", localHost, FRACheckerPort), Boolean.class);
        }, 12000);

        int fraSentinelPort = 32222;
        testSentinel(FRA_IDC, fraSentinelPort, checker);
    }

    @Test
    public void TestConsoleCheckerCleanSentinel() throws Exception {

        ZkServerMeta jqZk = getZk(JQ_IDC);
        startZk(jqZk);

        ZkServerMeta fraZk = getZk(FRA_IDC);
        startZk(fraZk);

        Map<Long, SentinelMeta> sentinel_metas = getXpipeMeta().getDcs().get(FRA_IDC).getSentinels();
        sentinel_metas.entrySet().stream().forEach(sentinel_meta -> {
            List<RedisStartCmd> sentinels = startSentinels(sentinel_meta.getValue());
            for(RedisStartCmd sentinel: sentinels) {
                try {
                    waitConditionUntilTimeOut(() -> sentinel.isProcessAlive(), 1000);
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
            }
        });


        int JQConsolePort = 18080;
        int FRACheckerPort = 18082;
        Map<String, String> consoles = new HashMap<>();
        consoles.put("jq", "http://127.0.0.1:" + JQConsolePort);
        consoles.put("fra", "http://127.0.0.1:" + JQConsolePort);
        Map<String, String> metaServers = new HashMap<>();
        Map<String, String> extraOptions = new HashMap<>();
        extraOptions.put(KEY_CLUSTER_SHARD_FOR_MIGRATE_SYS_CHECK, "cluster-dr,cluster-dr-shard1");
        extraOptions.put(KEY_SERVER_MODE, CONSOLE.name());
        extraOptions.put("console.cluster.types", "one_way,bi_direction,ONE_WAY,BI_DIRECTION");
        logger.info("========== start jq console ============");
        ConfigurableApplicationContext checker = startSpringConsoleChecker(FRACheckerPort, FRA_IDC, fraZk.getAddress(), Collections.singletonList("127.0.0.1:" + FRACheckerPort), consoles, metaServers, extraOptions);
        
        waitConditionUntilTimeOut(() -> {
            Map<String, Object> healthInfo = restOperations.getForObject(String.format("http://%s:%d/health", localHost, FRACheckerPort), Map.class);
            return (boolean)healthInfo.get("isLeader");
        }, 12000);
        
        int fraSentinelPort = 32222;
        testSentinel(FRA_IDC, fraSentinelPort, checker);

    }
    
    public void testSentinel(String idc, int sentinel_port, ConfigurableApplicationContext checker) throws Exception {
        
        SimpleObjectPool<NettyClient> clientPool = pool.getKeyPool(new DefaultEndPoint(localHost, sentinel_port));
        String addResult = new AbstractSentinelCommand.SentinelAdd(clientPool, sentinelMaster, localHost, localPort, 3, scheduled).execute().get();
        HostPort master = new AbstractSentinelCommand.SentinelMaster(clientPool, scheduled, sentinelMaster).execute().get();
        Assert.assertEquals(master.getHost(), localHost);
        Assert.assertEquals(master.getPort(), localPort);
        waitConditionUntilTimeOut(() -> {
            HostPort port = null;
            try {
                port = new AbstractSentinelCommand.SentinelMaster(clientPool, scheduled, sentinelMaster).execute().get();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return port == null;
        }, waitTime, 1000);
        checker.close();
        addResult = new AbstractSentinelCommand.SentinelAdd(clientPool, sentinelMaster, localHost, localPort, 3, scheduled).execute().get();
        master = new AbstractSentinelCommand.SentinelMaster(clientPool, scheduled, sentinelMaster).execute().get();
        Assert.assertEquals(master.getHost(), localHost);
        Assert.assertEquals(master.getPort(), localPort);
        Thread.currentThread().sleep(waitTime);
        master = new AbstractSentinelCommand.SentinelMaster(clientPool, scheduled, sentinelMaster).execute().get();
        Assert.assertEquals(master.getHost(), localHost);
        Assert.assertEquals(master.getPort(), localPort);
    }

    
    
    @After
    public void testAfter() throws Exception {
        stopAllServer();
    }
}


