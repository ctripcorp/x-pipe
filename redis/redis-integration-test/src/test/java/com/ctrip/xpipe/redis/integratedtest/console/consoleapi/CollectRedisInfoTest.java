package com.ctrip.xpipe.redis.integratedtest.console.consoleapi;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo.InfoActionContext;
import com.ctrip.xpipe.redis.core.foundation.IdcUtil;
import com.ctrip.xpipe.redis.integratedtest.console.AbstractXPipeClusterTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Slight
 * <p>
 * Jun 03, 2021 4:31 PM
 */
public class CollectRedisInfoTest extends AbstractXPipeClusterTest {

    private Map<String, String> metaServers;

    private Map<String, String> consoles;

    private String zkJQ;

    private String zkOY;

    @Before
    public void setUpRedisInfoTest() {
        metaServers = new HashMap<>();

        consoles = new HashMap<>();
        consoles.put("jq", "http://127.0.0.1:8080");
        consoles.put("oy", "http://127.0.0.1:8081");

        zkJQ = "127.0.0.1:" + IdcUtil.JQ_ZK_PORT;
        zkOY = "127.0.0.1:" + IdcUtil.OY_ZK_PORT;
    }

    @After
    public void afterCollectRedisInfoTest() throws IOException {
        cleanupAllSubProcesses();
        killAllRedisServers();
        cleanupConf();
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/xpipe-dr.sql");
    }

    @Test
    public void testFullFeaturedConsole() throws Exception {

        startZk(IdcUtil.JQ_ZK_PORT);
        startZk(IdcUtil.OY_ZK_PORT);

        setUpTestDataSource();

        startRedis(6379);
        startRedis(7379);

        startConsole(8080, "jq", zkJQ, Collections.singletonList("127.0.0.1:8080"), consoles, metaServers);
        startConsole(8081, "oy", zkOY, Collections.singletonList("127.0.0.1:8081"), consoles, metaServers);

        waitConditionUntilTimeOut(this::isAllProcessAlive);

        waitForServerResp("http://127.0.0.1:8080/api/health/redis/info/127.0.0.1/6379", InfoActionContext.Result.class, 60000,
                (health)-> "master".equals(((InfoActionContext.Result) health).getPayload().get("role")));

        waitForServerResp("http://127.0.0.1:8080/api/health/redis/info/all", InfoActionContext.ResultMap.class, 60000,
                (healthAll)-> "master".equals(((InfoActionContext.ResultMap) healthAll).get(HostPort.fromString("127.0.0.1:6379")).getPayload().get("role")));

        waitForServerResp("http://127.0.0.1:8081/api/redis/info/global", InfoActionContext.ResultMap.class, 60000,
                (all)-> "master".equals(((InfoActionContext.ResultMap) all).get(HostPort.fromString("127.0.0.1:6379")).getPayload().get("role")));
    }

    @Test
    public void testCheckerAndConsole() throws Exception {

        startZk(IdcUtil.JQ_ZK_PORT);
        startZk(IdcUtil.OY_ZK_PORT);

        setUpTestDataSource();

        startRedis(6379);
        startRedis(7379);

        startStandaloneConsole(8080, "jq", zkJQ, Collections.singletonList("127.0.0.1:8080"), consoles, Collections.emptyMap());
        startChecker(18080, "jq", zkJQ, Collections.singletonList("127.0.0.1:8080"));

        startStandaloneConsole(8081, "oy", zkOY, Collections.singletonList("127.0.0.1:8081"), consoles, Collections.emptyMap());
        startChecker(18081, "oy", zkOY, Collections.singletonList("127.0.0.1:8081"));

        waitConditionUntilTimeOut(this::isAllProcessAlive);

        waitForServerResp("http://127.0.0.1:18080/api/health/redis/info/127.0.0.1/6379", InfoActionContext.Result.class, 1200000,
                (health)-> "master".equals(((InfoActionContext.Result) health).getPayload().get("role")));

        waitForServerResp("http://127.0.0.1:18080/api/health/redis/info/all", InfoActionContext.ResultMap.class, 60000,
                (healthAll)-> "master".equals(((InfoActionContext.ResultMap) healthAll).get(HostPort.fromString("127.0.0.1:6379")).getPayload().get("role")));

        waitForServerResp("http://127.0.0.1:8081/api/redis/info/global", InfoActionContext.ResultMap.class, 60000,
                (all)-> "master".equals(((InfoActionContext.ResultMap) all).get(HostPort.fromString("127.0.0.1:6379")).getPayload().get("role")));
    }
}
