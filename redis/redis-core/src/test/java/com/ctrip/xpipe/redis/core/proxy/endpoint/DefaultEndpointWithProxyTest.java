package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * Jun 01, 2018
 */
public class DefaultEndpointWithProxyTest {

    private ProxyProtocol protocol = new DefaultProxyConnectProtocolParser().read("PROXY ROUTE TCP://127.0.0.1:6379\r\n");

    private static final Logger logger = LoggerFactory.getLogger(DefaultEndpointWithProxyTest.class);

    @Test
    public void testToString() {
        Endpoint endpoint = new DefaultEndPoint("127.0.0.1", 6379, (ProxyConnectProtocol) protocol);
        logger.info("{}", endpoint);
        logger.info("{}", endpoint.toString());
        logger.info("{}", endpoint.getProxyProtocol());
    }

    @Test
    public void testEquals() {
        Endpoint endpoint1 = new DefaultEndPoint("127.0.0.1", 6379, (ProxyConnectProtocol) protocol);
        Endpoint endpoint2 = new DefaultEndPoint("127.0.0.1", 6379);

        logger.info("{}", ObjectUtils.equals(endpoint1, endpoint2));
        Assert.assertFalse(ObjectUtils.equals(endpoint1, endpoint2));
    }

    @Test
    public void testEncode() {
        Endpoint endpoint = new DefaultEndPoint("127.0.0.1", 6379, (ProxyConnectProtocol) protocol);
        Codec.DEFAULT.encode(endpoint);
    }
}