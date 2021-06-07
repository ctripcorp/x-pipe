package com.ctrip.xpipe.redis.integratedtest.console.leader;

import com.ctrip.xpipe.redis.checker.controller.CheckerHealthController;
import com.ctrip.xpipe.redis.core.foundation.IdcUtil;
import com.ctrip.xpipe.redis.integratedtest.console.AbstractXPipeClusterTest;
import com.ctrip.xpipe.redis.integratedtest.console.ForkProcessCmd;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;

import java.io.IOException;
import java.util.*;

import static com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector.KEY_CONSOLE_ID;

/**
 * @author lishanglin
 * date 2021/2/3
 */
public class ConsoleLeaderTest extends AbstractXPipeClusterTest {

    private String dcJQ = "jq";

    private String zkJQ;

    private List<String> consoles;

    private String consoleId8080 = "jq8080";
    private String consoleId8081 = "jq8081";

    private ForkProcessCmd jqConsole8080;
    private ForkProcessCmd jqConsole8081;

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/single-dc.sql");
    }

    @Before
    public void setupConsoleLeaderTest() {
        zkJQ = "127.0.0.1:" + IdcUtil.JQ_ZK_PORT;
        consoles = new ArrayList<>();
        consoles.add("127.0.0.1:8080");
        consoles.add("127.0.0.1:8081");
    }

    @After
    public void afterDRTest() throws IOException {
        cleanupAllSubProcesses();
        killAllRedisServers();
        cleanupConf();
    }

    @Test
    public void testOriginLeaderRestart() throws Exception {
        startJqConsoleClusters();

        boolean is8080Leader = isLeader(8080);
        boolean is8081Leader = isLeader(8081);

        Assert.assertTrue(is8080Leader ^ is8081Leader);

        ForkProcessCmd originLeaderConsole = is8080Leader ? jqConsole8080 : jqConsole8081;
        int newLeaderPort = is8080Leader ? 8081 : 8080;
        int newFollowerPort = is8080Leader ? 8080 : 8081;

        stopServer(originLeaderConsole);
        waitConditionUntilTimeOut(() -> isLeader(newLeaderPort), 10000, 1000);

        startConsole(newFollowerPort, dcJQ, zkJQ, consoles, Collections.emptyMap(), Collections.emptyMap(), Collections.singletonMap(KEY_CONSOLE_ID, dcJQ + newFollowerPort));

        waitForServerAck(String.format("http://127.0.0.1:%d/health", newFollowerPort), Map.class, 120000);

        Assert.assertTrue(isLeader(newLeaderPort));
        Assert.assertFalse(isLeader(newFollowerPort));

        waitConditionUntilTimeOut(()-> {
            try {
                return getCheckInstance(newLeaderPort).getActions().size() > getCheckInstance(newFollowerPort).getActions().size();
            } catch (Exception e) {
                return false;
            }
        }, 30000, 100);
    }

    private boolean isLeader(int port) {
        Map<String, Object> healthState = restTemplate.exchange(String.format("http://127.0.0.1:%d/health", port),
                HttpMethod.GET, null, new ParameterizedTypeReference<Map<String, Object>>() {}).getBody();

        return (Boolean) healthState.get("isLeader");
    }

    private CheckerHealthController.HealthCheckInstanceModel getCheckInstance(int port) throws Exception {
        String url = String.format("http://127.0.0.1:%d/api/health/check/instance/127.0.0.1/6379", port);
        waitForServerAck(url, CheckerHealthController.HealthCheckInstanceModel.class, 30000);
        return restTemplate.getForObject(url, CheckerHealthController.HealthCheckInstanceModel.class);
    }

    private void startJqConsoleClusters() throws Exception {
        startZk(IdcUtil.JQ_ZK_PORT);

        setUpTestDataSource();

        startRedis(6379);

        jqConsole8080 = startConsole(8080, dcJQ, zkJQ, consoles, Collections.emptyMap(), Collections.emptyMap(), Collections.singletonMap(KEY_CONSOLE_ID, consoleId8080));
        jqConsole8081 = startConsole(8081, dcJQ, zkJQ, consoles, Collections.emptyMap(), Collections.emptyMap(), Collections.singletonMap(KEY_CONSOLE_ID, consoleId8081));

        waitForServerAck("http://127.0.0.1:8080/health", Map.class, 120000);
        waitForServerAck("http://127.0.0.1:8081/health", Map.class, 60000);
    }

}
