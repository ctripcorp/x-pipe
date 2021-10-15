package com.ctrip.xpipe.redis.integratedtest.checker;

import com.ctrip.framework.xpipe.redis.ProxyRegistry;
import com.ctrip.framework.xpipe.redis.proxy.ProxyResourceManager;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ZkServerMeta;
import com.ctrip.xpipe.redis.core.proxy.command.AbstractProxyMonitorCommand;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelSocketStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelStatsResult;
import com.ctrip.xpipe.redis.integratedtest.metaserver.AbstractXpipeServerMultiDcTest;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition.KEY_SERVER_MODE;
import static com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition.SERVER_MODE.CONSOLE;
import static com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig.KEY_CLUSTER_SHARD_FOR_MIGRATE_SYS_CHECK;

public class CheckCrdtRedisByProxy extends AbstractXpipeServerMultiDcTest {

    @BeforeClass
    public static void beforeCheckCrdtRedisByProxy() {
        System.setProperty("DisableLoadProxyAgentJar", "false");
    }
    
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/xpipe-crdt-checker-proxy.sql");
    }

    private final int consolePort = 18080;
    
    private final int checkerPort = 18000;
    
    @Before
    public void startServers() throws Exception {

        startProxy( JQ_IDC, 11080, 11443);
        startProxy( FRA_IDC,11081, 11444);
        
        startDb();
        
        XpipeNettyClientKeyedObjectPool pool = getXpipeNettyClientKeyedObjectPool();
        
        RedisMeta jqMasterInfo = getMasterRedis(JQ_IDC, crdtClusterName, crdtShardName);
        
        List<CrdtRedisServer> masters = Lists.newArrayList();
        masters.add(new CrdtRedisServer(getGid(JQ_IDC), jqMasterInfo));
        RedisMeta fraMasterInfo = getMasterRedis(FRA_IDC, crdtClusterName, crdtShardName);
        masters.add(new CrdtRedisServer(getGid(FRA_IDC), fraMasterInfo));
        startCrdtMasters(masters , pool, scheduled);
        
        
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

    }
    
    @Test
    public void checkHealth() throws Exception {
        waitConditionUntilTimeOut(() -> {
            Map<String, Map<HostPort, Object>> result = restOperations.getForObject("http://" + LOCAL_HOST + ":" + consolePort + "/console/cross-master/delay/bi_direction/" + JQ_IDC + "/cluster1/shard1", Map.class);
            if(result == null || result.get(FRA_IDC) == null || result.get(FRA_IDC).size() == 0) return false;
            for(Object value : result.get(FRA_IDC).values()) {
                if(value instanceof  Integer) {
                    int v = (int)value;
                    if(v < 0 || v >= 999000) {
                        return false;
                    }
                } else if(value instanceof Long) {
                    long v = (long)value;
                    if(v < 0 || v >= 999000) {
                        return false;
                    }
                }
            }
            return true;
        }, 50000, 1000);
        XpipeNettyClientKeyedObjectPool pool = getXpipeNettyClientKeyedObjectPool();
        waitConditionUntilTimeOut(() -> {
            try {
                TunnelSocketStatsResult[] result = new AbstractProxyMonitorCommand.ProxyMonitorSocketStatsCommand(pool.getKeyPool(new DefaultEndPoint("127.0.0.1", 11081)),scheduled).execute().get();
                return result.length > 1;
            } catch (Exception ignore) {
            } 
            return false;
        }, 5000, 100);
    }

    @After
    public void stopServers() throws Exception {
        stopAllServer();
        ProxyRegistry.unregisterProxy("127.0.0.1", 38379);
    }
}
