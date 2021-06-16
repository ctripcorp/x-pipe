package com.ctrip.xpipe.redis.integratedtest.metaserver;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.model.RouteModel;
import com.ctrip.xpipe.redis.core.protocal.cmd.*;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.ProxyRedisMeta;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.web.client.RestOperations;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class CRDTTest extends AbstractMetaServerMultiDcTest {



    @Before
    public void testBefore() throws Exception {
        startCRDTAllServer();

    }

    void ChangeRoute(String uri, RouteModel model) {
        restTemplate.put(String.format("http://%s/api/route", uri), model);
    }


    @Test
    public void RroxyTest() throws Exception {
        String jqConsoleUrl = "127.0.0.1:18080";
        String fraConsoleUrl = "127.0.0.1:18082";
        XpipeNettyClientKeyedObjectPool pool = getXpipeNettyClientKeyedObjectPool();
        Endpoint master = new DefaultEndPoint("127.0.0.1", 36379);
        CheckCrdtHealth h = new CheckCrdtHealth(jqConsoleUrl, "jq", "cluster1", "shard1");
        waitConditionUntilTimeOut(h::checkConsoleHealth, 100000, 1000);
        h = new CheckCrdtHealth(fraConsoleUrl, "fra", "cluster1", "shard1");
        waitConditionUntilTimeOut(h::checkConsoleHealth, 100000, 1000);
        CheckerPeer p = new CheckerPeer(pool.getKeyPool(master), scheduled);
        p.setHadPeerParams("127.0.0.1", 38379);
        p.setProxySize("127.0.0.1", 38379, 2);
        waitConditionUntilTimeOut(p::checkHadPeer, 100000, 1000);

        PeerOfCommand peerOfCommand = new PeerOfCommand(pool.getKeyPool(master), getGid("fra"), null, 0, scheduled);
        peerOfCommand.execute().get();
        Assert.assertEquals(p.checkHadPeer(), false);
        waitConditionUntilTimeOut(p::checkHadPeer, 100000, 1000);


        Assert.assertEquals(p.checkProxySize(), true);
        RouteModel model = new RouteModel();
        model.setId(2).setSrcProxyIds("2").setTag("META").setDstProxyIds("1,5").setSrcDcName("jq").setDstDcName("fra").setActive(true);
        ChangeRoute(jqConsoleUrl, model);
        p.setProxySize("127.0.0.1", 38379,1);
        waitConditionUntilTimeOut(p::checkProxySize, 100000, 1000);
        model.setSrcProxyIds("2,6");
        ChangeRoute(jqConsoleUrl, model);
        p.setProxySize("127.0.0.1", 38379,2);
        waitConditionUntilTimeOut(p::checkProxySize, 100000, 1000);

        Endpoint master_point= new DefaultEndPoint("127.0.0.1", 38380);
        Endpoint slave_point= new DefaultEndPoint("127.0.0.1", 38379);
        Command<String> command = new DefaultSlaveOfCommand(
                pool.getKeyPool(master_point),
                scheduled);
        command.execute();
        command = new DefaultSlaveOfCommand(pool.getKeyPool(slave_point), master_point.getHost(), master_point.getPort(), scheduled);
        command.execute();
        p.setProxySize("127.0.0.1", 38380,2);
        waitConditionUntilTimeOut(p::checkProxySize, 100000, 1000);

    }

    @After
    public void testAfter() {
        stopAllServer();
    }
}
