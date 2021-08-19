package com.ctrip.xpipe.redis.integratedtest.metaserver.scenes;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.redis.core.protocal.cmd.*;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.integratedtest.metaserver.AbstractMetaServerMultiDcTest;
import com.ctrip.xpipe.redis.integratedtest.metaserver.scenes.tools.RedisChecker;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition.SERVER_MODE.CONSOLE;

public class CrdtPeerMasterChangeTest extends AbstractMetaServerMultiDcTest{
    public Map<String, AbstractMetaServerMultiDcTest.ConsoleInfo> defaultConsoleInfo() {
        Map<String, AbstractMetaServerMultiDcTest.ConsoleInfo> consoleInfos = new HashMap<>();
        //start console + checker 2server
        consoleInfos.put("jq", new AbstractMetaServerMultiDcTest.ConsoleInfo(CONSOLE).setConsole_port(18080).setChecker_port(28080));
        //start conset_checker 1server
        consoleInfos.put("oy", new AbstractMetaServerMultiDcTest.ConsoleInfo(CONSOLE).setConsole_port(18081).setChecker_port(28081));
        //start checker 1 server
        consoleInfos.put("fra", new AbstractMetaServerMultiDcTest.ConsoleInfo(CONSOLE).setConsole_port(18082).setChecker_port(28082));
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
    public void testPeerMasterChange() throws Exception {
        XpipeNettyClientKeyedObjectPool pool = getXpipeNettyClientKeyedObjectPool();
        Endpoint master = new DefaultEndPoint("127.0.0.1", 36379);
        RedisChecker redisChecker = new RedisChecker(pool, scheduled);
        Pair<Long, Endpoint> jq2fraPeerInfo = new Pair<Long, Endpoint>(5L, createProxyEndpoint("127.0.0.1", 38379, "PROXY ROUTE PROXTCP://127.0.0.1:11081,PROXTCP://127.0.0.1:11083 PROXYTLS://127.0.0.1:11443,PROXYTLS://127.0.0.1:11445"));
        waitConditionUntilTimeOut(redisChecker.containsPeer(master, jq2fraPeerInfo) , 200000, 1000);
        waitConditionUntilTimeOut(redisChecker.containsPeer(master, jq2fraPeerInfo) , 10000, 1000);
        //remove peer
        PeerOfCommand peerOfCommand = new PeerOfCommand(pool.getKeyPool(master), 5L, null,  scheduled);
        peerOfCommand.execute().get();
        Assert.assertEquals(redisChecker.containsPeer(master, jq2fraPeerInfo).getAsBoolean(), false);
        //will add peer
        waitConditionUntilTimeOut(redisChecker.containsPeer(master, jq2fraPeerInfo), 100000, 1000);

        //change fq peer master
        Endpoint fqMasterEndPint = new DefaultEndPoint("127.0.0.1", 38379);
        Endpoint fqSlaveEndPint = new DefaultEndPoint("127.0.0.1", 38380);
        Command command = new DefaultSlaveOfCommand(pool.getKeyPool(fqMasterEndPint), fqSlaveEndPint.getHost(), fqSlaveEndPint.getPort(), scheduled);
        command.execute();
        command = new DefaultSlaveOfCommand(pool.getKeyPool(fqSlaveEndPint), scheduled);
        command.execute();
        jq2fraPeerInfo = new Pair<Long, Endpoint>(5L, createProxyEndpoint("127.0.0.1", 38380, "PROXY ROUTE PROXTCP://127.0.0.1:11081,PROXTCP://127.0.0.1:11083 PROXYTLS://127.0.0.1:11443,PROXYTLS://127.0.0.1:11445"));
        waitConditionUntilTimeOut(redisChecker.containsPeer(master, jq2fraPeerInfo) , 200000, 1000);



    }

    @After
    public void testAfter() throws Exception {
        stopAllServer();
    }
}
