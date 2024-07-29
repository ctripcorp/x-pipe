package com.ctrip.xpipe.redis.integratedtest.console;

import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.foundation.IdcUtil;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.ctrip.xpipe.redis.checker.config.impl.ConsoleConfigBean.KEY_CLUSTER_SHARD_FOR_MIGRATE_SYS_CHECK;

/**
 * @author lishanglin
 * date 2021/6/3
 */
public class AbstractXPipeDrTest extends AbstractXPipeClusterTest {

    protected Map<String, DcInfo> dcInfos;

    protected Map<String, String> consoles;

    protected Map<String, String> metaservers;

    protected ForkProcessCmd jqMetaServer;
    protected ForkProcessCmd oyMetaServer;

    protected String zkJQ;

    protected String zkOY;

    @Before
    public void setupDRTest() {
        dcInfos = new HashMap<>();
        dcInfos.put("jq", new DcInfo("http://127.0.0.1:" + IdcUtil.JQ_METASERVER_PORT));
        dcInfos.put("oy", new DcInfo("http://127.0.0.1:" + IdcUtil.OY_METASERVER_PORT));

        consoles = new HashMap<>();
        consoles.put("jq", "http://127.0.0.1:8080");
        consoles.put("oy", "http://127.0.0.1:8081");

        metaservers = new HashMap<>();
        metaservers.put("jq", dcInfos.get("jq").getMetaServerAddress());
        metaservers.put("oy", dcInfos.get("oy").getMetaServerAddress());

        zkJQ = "127.0.0.1:" + IdcUtil.JQ_ZK_PORT;
        zkOY = "127.0.0.1:" + IdcUtil.OY_ZK_PORT;
    }

    @After
    public void afterDRTest() throws IOException {
        cleanupAllSubProcesses();
        killAllRedisServers();
        cleanupConf();
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/xpipe-dr.sql");
    }

    protected void startSimpleXPipeDR() throws Exception {
        startZk(IdcUtil.JQ_ZK_PORT);
        startZk(IdcUtil.OY_ZK_PORT);

        setUpTestDataSource(); // init data in h2

        startRedis(6379);
        startRedis(7379);

        Map<String, String> extraOptions = Collections.singletonMap(KEY_CLUSTER_SHARD_FOR_MIGRATE_SYS_CHECK, "cluster-dr,cluster-dr-shard1");
        startConsole(8080, "jq", zkJQ, Collections.singletonList("127.0.0.1:8080"), consoles, metaservers, extraOptions);
        startConsole(8081, "oy", zkOY, Collections.singletonList("127.0.0.1:8081"), consoles, metaservers, extraOptions);

        String userDir = System.getProperty("user.dir");
        startKeepercontainer("jq", zkJQ, 7080, userDir + "/src/test/tmp/keepercontainer7080");
        startKeepercontainer("jq", zkJQ, 7081, userDir + "/src/test/tmp/keepercontainer7081");
        startKeepercontainer("oy", zkOY, 7180, userDir + "/src/test/tmp/keepercontainer7180");
        startKeepercontainer("oy", zkOY, 7181, userDir + "/src/test/tmp/keepercontainer7181");

        // wait for console init
        waitForServerAck("http://127.0.0.1:8080/api/dc/jq", DcMeta.class, 120000);
        waitForServerAck("http://127.0.0.1:8081/api/dc/oy", DcMeta.class, 60000);

        checkAllProcessAlive();

        jqMetaServer = startMetaServer("jq", "http://127.0.0.1:8080", zkJQ, IdcUtil.JQ_METASERVER_PORT, dcInfos);
        oyMetaServer = startMetaServer("oy", "http://127.0.0.1:8081", zkOY, IdcUtil.OY_METASERVER_PORT, dcInfos);

        // repl online
        waitForServerRespAsExpected("http://127.0.0.1:8080/api/health/127.0.0.1/7379", String.class, "\"HEALTHY\"", 120000);

        checkAllProcessAlive();
    }

}
