package com.ctrip.xpipe.redis.integratedtest.console.consoleapi;

import com.ctrip.xpipe.redis.checker.controller.result.ActionContextRetMessage;
import com.ctrip.xpipe.redis.core.foundation.IdcUtil;
import com.ctrip.xpipe.redis.integratedtest.console.AbstractXPipeClusterTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector.KEY_CONSOLE_ID;
import static org.junit.Assert.assertEquals;

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

    private String consoleId8080 = "jq8080";
    private String consoleId8081 = "jq8081";

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
    public void afterDRTest() throws IOException {
        cleanupAllSubProcesses();
        killAllRedisServers();
        cleanupConf();
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/xpipe-dr.sql");
    }

    public static class Message extends ActionContextRetMessage<Map<String, String>> {

        public Message() {

        }
    }

    public static class MessageMap extends HashMap<String, ActionContextRetMessage<Map<String, String>>> {

        public MessageMap() {
        }

    }

    @Test
    public void testStandaloneConsole() throws Exception {

        startZk(IdcUtil.JQ_ZK_PORT);
        startZk(IdcUtil.OY_ZK_PORT);

        startH2Server();
        setUpTestDataSource();

        startRedis(6379);
        startRedis(7379);

        startConsole(8080, "jq", zkJQ, Collections.singletonList("127.0.0.1:8080"), consoles, metaServers, Collections.singletonMap(KEY_CONSOLE_ID, consoleId8080));
        startConsole(8081, "oy", zkOY, Collections.singletonList("127.0.0.1:8081"), consoles, metaServers, Collections.singletonMap(KEY_CONSOLE_ID, consoleId8081));

        waitConditionUntilTimeOut(this::isAllProcessAlive);

        Message single = (Message) waitForServerResp("http://127.0.0.1:8080/api/redis/info/127.0.0.1/6379", Message.class, 60000);

        assertEquals("master", single.getPayload().get("role"));

        MessageMap all = (MessageMap) waitForServerResp("http://127.0.0.1:8080/api/redis/info/all", MessageMap.class, 60000);

        assertEquals("master", all.get("127.0.0.1:6379").getPayload().get("role"));
    }

    @Test
    public void testCheckerAndConsole() {
    }
}
