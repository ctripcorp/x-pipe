package com.ctrip.xpipe.redis.integratedtest.metaserver.scenes;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.redis.console.model.RouteModel;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.integratedtest.metaserver.AbstractMetaServerMultiDcTest;
import com.ctrip.xpipe.redis.integratedtest.metaserver.scenes.tools.ConsoleService;
import com.ctrip.xpipe.redis.integratedtest.metaserver.scenes.tools.RedisChecker;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition.SERVER_MODE.CONSOLE;

/**
 *  when route change
 *  peer proxy will change
 *  metaserver send peerof command
 */
public class RouteChangeTest extends AbstractMetaServerMultiDcTest {
    public Map<String, ConsoleInfo> defaultConsoleInfo() {
        Map<String, ConsoleInfo> consoleInfos = new HashMap<>();
        //start console + checker 2server
        consoleInfos.put("jq", new ConsoleInfo(CONSOLE).setConsole_port(18080).setChecker_port(28080));
        //start conset_checker 1server
        consoleInfos.put("oy", new ConsoleInfo(CONSOLE).setConsole_port(18081).setChecker_port(28081));
        //start checker 1 server
        consoleInfos.put("fra", new ConsoleInfo(CONSOLE).setConsole_port(18082).setChecker_port(28082));
        return consoleInfos;
    }
    @Before
    public void testBefore() throws Exception {
        stopAllServer();
        startCRDTAllServer(defaultConsoleInfo());
    }





    ProxyEnabledEndpoint createProxyEndpoint(String host, int port, String proxy) {
        ProxyConnectProtocol protocol = new DefaultProxyConnectProtocolParser().read(proxy);
        return new ProxyEnabledEndpoint(host, port, protocol);
    }

    @Test
    public void testRouteChange() throws Exception {

        String jqConsoleUrl = "127.0.0.1:18080";
        XpipeNettyClientKeyedObjectPool pool = getXpipeNettyClientKeyedObjectPool();
        Endpoint master = new DefaultEndPoint("127.0.0.1", 36379);

        Pair<Long, Endpoint> jq2fraPeerInfo = new Pair<Long, Endpoint>(5L, createProxyEndpoint("127.0.0.1", 38379, "PROXY ROUTE PROXTCP://127.0.0.1:11081,PROXTCP://127.0.0.1:11083 PROXYTLS://127.0.0.1:11443,PROXYTLS://127.0.0.1:11445"));
        ConsoleService jqService = new ConsoleService("jq", jqConsoleUrl);
        RedisChecker redisChecker = new RedisChecker(pool, scheduled);
        waitConditionUntilTimeOut(redisChecker.containsPeer(master, jq2fraPeerInfo) , 200000, 1000);
        waitConditionUntilTimeOut(redisChecker.containsPeer(master, jq2fraPeerInfo) , 10000, 1000);

        RouteModel model = new RouteModel();
        model.setId(2).setSrcProxyIds("2").setTag("META").setDstProxyIds("1,5").setSrcDcName("jq").setDstDcName("fra").setActive(true);
        jqService.changeRoute(model);
        jq2fraPeerInfo = new Pair<Long, Endpoint>(5L, createProxyEndpoint("127.0.0.1", 38379, "PROXY ROUTE PROXTCP://127.0.0.1:11081 PROXYTLS://127.0.0.1:11443,PROXYTLS://127.0.0.1:11445"));
        waitConditionUntilTimeOut(redisChecker.containsPeer(master, jq2fraPeerInfo) , 61000, 1000);
        model.setDstProxyIds("1");
        jqService.changeRoute(model);

        jq2fraPeerInfo = new Pair<Long, Endpoint>(5L, createProxyEndpoint("127.0.0.1", 38379, "PROXY ROUTE PROXTCP://127.0.0.1:11081 PROXYTLS://127.0.0.1:11443"));
        waitConditionUntilTimeOut(redisChecker.containsPeer(master, jq2fraPeerInfo) , 61000, 1000);

    }

    @After
    public void testAfter() throws Exception {
        stopAllServer();
    }



}
