package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 16, 2018
 */
public class KeeperCommandTest extends AbstractRedisTest {

    @Test
    public void testSetState() throws Exception {

        Server server = startServer("+OK\r\n");

        String result = new AbstractKeeperCommand.KeeperSetStateCommand(
                new KeeperMeta().setIp("localhost").setPort(server.getPort()),
                KeeperState.ACTIVE, new Pair<>("localhost", randomPort()),
                scheduled
        ).execute().get(3, TimeUnit.SECONDS);
        Assert.assertEquals("OK", result);
    }

    @Test
    public void testSetStateRoute() throws Exception {

        Server server = startServer("+OK\r\n");

        String result = new AbstractKeeperCommand.KeeperSetStateCommand(
                new KeeperMeta().setIp("localhost").setPort(server.getPort()),
                KeeperState.ACTIVE, new Pair<>("localhost", randomPort()),
                new RouteMeta().setRouteInfo("PROXYTCP://1.1.1.1:80,PROXYTCP://1.1.1.2:80 PROXYTLS://1.1.1.5:443,PROXYTLS://1.1.1.6:443"),
                scheduled
        ).execute().get(3, TimeUnit.SECONDS);

        Assert.assertEquals("OK", result);
    }


}
