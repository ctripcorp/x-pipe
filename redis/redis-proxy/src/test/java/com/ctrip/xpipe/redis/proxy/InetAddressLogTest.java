package com.ctrip.xpipe.redis.proxy;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2018
 */
public class InetAddressLogTest {

    private Logger logger = LoggerFactory.getLogger(InetAddressLogTest.class);

    @Test
    public void test() {
        InetSocketAddress address = new InetSocketAddress("www.baidu.com", 52156);
        logger.info("[getHostString] {}", address.getHostString());
        logger.info("[getHostName] {}", address.getHostName());
        logger.info("[getHostAddress] {}", address.getAddress().getHostAddress());
        logger.info("[isUnresolved] {}", address.isUnresolved());

    }
}
