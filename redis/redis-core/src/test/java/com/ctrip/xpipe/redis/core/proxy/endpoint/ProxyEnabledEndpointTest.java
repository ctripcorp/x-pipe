package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.redis.core.proxy.DefaultProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.DefaultProxyProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Jun 01, 2018
 */
public class ProxyEnabledEndpointTest {

    private ProxyProtocol protocol = new DefaultProxyProtocolParser().read("PROXY ROUTE TCP://127.0.0.1:6379\r\n");

    private static final Logger logger = LoggerFactory.getLogger(ProxyEnabledEndpointTest.class);
    @Test
    public void testToString() {
        ProxyEnabledEndpoint endpoint = new ProxyEnabledEndpoint("127.0.0.1", 6379, protocol);
        logger.info("{}", endpoint);
        logger.info("{}", endpoint.toString());
        logger.info("{}", endpoint.getProxyProtocol());
    }
}