package com.ctrip.xpipe.redis.core.proxy.parser;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.parser.path.ForwardForOptionParser;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class DefaultProxyConnectProtocolParserTest {

    private DefaultProxyConnectProtocolParser parser = new DefaultProxyConnectProtocolParser();

    @Test
    public void newProxyProtocol() {
        ProxyProtocol protocol = parser.newProxyProtocol("");
        Assert.assertNotNull(protocol);
        Assert.assertTrue(protocol instanceof ProxyConnectProtocol);
    }

    @Test
    public void addProxyOptionParser() {
        Assert.assertTrue(parser.getParsers().isEmpty());
        parser.addProxyOptionParser(new ForwardForOptionParser());
        Assert.assertFalse(parser.getParsers().isEmpty());
        Assert.assertTrue(parser.getParsers().get(0) instanceof ForwardForOptionParser);
    }

    @Test
    public void removeProxyOptionParser() {
        Assert.assertTrue(parser.getParsers().isEmpty());
        parser.addProxyOptionParser(new ForwardForOptionParser());
        Assert.assertFalse(parser.getParsers().isEmpty());
        ForwardForOptionParser forwardForOptionParser = (ForwardForOptionParser) parser.getProxyOptionParser(PROXY_OPTION.FORWARD_FOR);
        Assert.assertNotNull(forwardForOptionParser);
        parser.removeProxyOptionParser(forwardForOptionParser);
        Assert.assertNull(parser.getProxyOptionParser(PROXY_OPTION.FORWARD_FOR));
    }
}