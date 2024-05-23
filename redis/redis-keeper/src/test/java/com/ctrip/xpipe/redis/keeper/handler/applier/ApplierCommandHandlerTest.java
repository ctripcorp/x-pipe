package com.ctrip.xpipe.redis.keeper.handler.applier;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author lishanglin
 * date 2022/8/11
 */
public class ApplierCommandHandlerTest extends AbstractTest {

    private ApplierCommandHandler handler;

    @Before
    public void setupApplierCommandHandlerTest() {
        handler = new ApplierCommandHandler();
    }

    @Test
    public void proxyProtocolParseTest() {
        String[] args = "setstate ACTIVE 127.0.0.1 6379 a1:1 PROXY ROUTE PROXYTCP://127.0.0.1:80,PROXYTCP://127.0.0.2:80 TCP".split(" ");
        ProxyConnectProtocol protocol = handler.getProxyProtocol(args, handler.findIndex(args, "PROXY"));
        Assert.assertEquals("PROXY ROUTE PROXYTCP://127.0.0.1:80,PROXYTCP://127.0.0.2:80 TCP://127.0.0.1:6379", protocol.getContent());

         args = "setstate ACTIVE 127.0.0.1 6379 a1:1 PROTOCOL PSYNC PROXY ROUTE PROXYTCP://127.0.0.1:80,PROXYTCP://127.0.0.2:80 TCP".split(" ");
        protocol = handler.getProxyProtocol(args, handler.findIndex(args, "PROXY"));
        Assert.assertEquals("PROXY ROUTE PROXYTCP://127.0.0.1:80,PROXYTCP://127.0.0.2:80 TCP://127.0.0.1:6379", protocol.getContent());
    }

    @Test
    public void enableGtidTest() {
        String[] args = "setstate ACTIVE 127.0.0.1 6379 a1:1 PROTOCOL XSYNC".split(" ");
        boolean enableGtid = handler.useXsync(args);
        Assert.assertTrue(enableGtid);

        args = "setstate ACTIVE 127.0.0.1 6379 a1:1 PROTOCOL PSYNC".split(" ");
        enableGtid = handler.useXsync(args);
        Assert.assertFalse(enableGtid);

        args = "setstate ACTIVE 127.0.0.1 6379 a1:1".split(" ");
        enableGtid = handler.useXsync(args);
        Assert.assertTrue(enableGtid);
    }
}
