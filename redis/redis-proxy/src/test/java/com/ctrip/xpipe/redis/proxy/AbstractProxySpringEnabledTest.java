package com.ctrip.xpipe.redis.proxy;

import org.junit.runner.RunWith;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = AbstractProxySpringEnabledTest.ProxyTestConfig.class)
public class AbstractProxySpringEnabledTest extends AbstractRedisProxyServerTest {

    @SpringBootApplication
    public static class ProxyTestConfig{

    }
}
