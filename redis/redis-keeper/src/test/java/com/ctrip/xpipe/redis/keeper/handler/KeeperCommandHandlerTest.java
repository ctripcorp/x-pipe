package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import org.junit.Assert;
import org.junit.Test;

import static com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser.WHITE_SPACE;
import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * May 23, 2018
 */
public class KeeperCommandHandlerTest {

    private KeeperCommandHandler handler = new KeeperCommandHandler();

    @Test
    public void testGetProxyProtocol() {
        final String command = "setstate ACTIVE 127.0.0.1 6379 PROXY ROUTE proxy://127.0.0.1:6379,tls://10.2.1.1:6379" +
                " tls://10.3.2.1:6379,tls://10.3.2.1:6380 raw://127.0.0.1:6380;FORWARD_FOR 192.168.1.1:6379";
        ProxyProtocol protocol = handler.getProxyProtocol(command.split(WHITE_SPACE));
        Assert.assertTrue(command.contains(protocol.getContent()));
        Assert.assertEquals("FORWARD_FOR 192.168.1.1:6379", protocol.getForwardFor());
    }
}