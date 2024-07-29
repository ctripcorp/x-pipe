package com.ctrip.xpipe.redis.integratedtest.checker;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.ZkServerMeta;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.RedisStartCmd;
import com.ctrip.xpipe.redis.integratedtest.metaserver.AbstractXpipeServerMultiDcTest;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import static com.ctrip.xpipe.redis.checker.config.impl.ConsoleConfigBean.KEY_CLUSTER_SHARD_FOR_MIGRATE_SYS_CHECK;
import static com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition.SERVER_MODE.CONSOLE;

public class TwoChecker2SameConsole extends AbstractXpipeServerMultiDcTest {
    
    private int jqConsolePort = 18080;
    private  int fraConsolePort = 18081;
    
    @Before 
    public void startServers() throws Exception {
        startDb();

        ZkServerMeta jqZk = getZk(JQ_IDC);
        startZk(jqZk);
        ZkServerMeta fraZk = getZk(FRA_IDC);
        startZk(fraZk);
        XpipeNettyClientKeyedObjectPool pool = getXpipeNettyClientKeyedObjectPool();

        List<CrdtRedisServer> masters = Lists.newArrayList();
        RedisMeta jqMasterInfo = getMasterRedis(JQ_IDC, crdtClusterName, crdtShardName);
        masters.add(new CrdtRedisServer(getGid(JQ_IDC), jqMasterInfo));
        RedisMeta fraMasterInfo = getMasterRedis(FRA_IDC, crdtClusterName, crdtShardName);
        masters.add(new CrdtRedisServer(getGid(FRA_IDC), fraMasterInfo));
        startCrdtMasters(masters , pool, scheduled);
        
//        
        Map<String, String> consoles = new HashMap<>();
        consoles.put("jq", "http://127.0.0.1:" + jqConsolePort);
        consoles.put("fra", "http://127.0.0.1:" + fraConsolePort);
        Map<String, String> metaServers = new HashMap<>();
        Map<String, String> extraOptions = new HashMap<>();
        extraOptions.put(KEY_CLUSTER_SHARD_FOR_MIGRATE_SYS_CHECK, "cluster-dr,cluster-dr-shard1");
        //extraOptions.put(KEY_SERVER_MODE, CONSOLE.name());
        extraOptions.put("console.cluster.types", "one_way,bi_direction,ONE_WAY,BI_DIRECTION");

        startSpringConsole(jqConsolePort, JQ_IDC, jqZk.getAddress(), Collections.singletonList("127.0.0.1:" + jqConsolePort), consoles, metaServers, extraOptions);

        startSpringConsoleChecker(fraConsolePort, FRA_IDC, fraZk.getAddress(), Collections.singletonList("127.0.0.1:" + fraConsolePort), consoles, metaServers, extraOptions);
        
        int checkerPort = 18001;
        startSpringChecker(checkerPort++, JQ_IDC, jqZk.getAddress(), Collections.singletonList("127.0.0.1:" + jqConsolePort), "127.0.0.2");

//        startSpringChecker(checkerPort++, FRA_IDC, fraZk.getAddress(), Collections.singletonList("127.0.0.1:" + fraConsolePort), "127.0.0.3");
        
    }
    
    public BooleanSupplier checkDelay(int consolePort, String srcIdc, String targetIdc) {
        return ()-> {
            Map<String, Map<HostPort, Object>> result = restOperations.getForObject("http://127.0.0.1:"+consolePort+"/console/cross-master/delay/bi_direction/"+srcIdc+"/cluster1/shard1", Map.class);
            if(result == null || result.get(targetIdc) == null) return false;
            for(Object value : result.get(targetIdc).values()) {
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
        };
    }
    
    @Test
    public void waitConsole() throws InterruptedException, TimeoutException {
        waitConditionUntilTimeOut(checkDelay(fraConsolePort, JQ_IDC, FRA_IDC), 50000, 1000);
        waitConditionUntilTimeOut(checkDelay(fraConsolePort, FRA_IDC, JQ_IDC), 50000, 1000);
    }
    
    @After
    public void stopServers() throws Exception {
        stopAllServer();
    }
}
