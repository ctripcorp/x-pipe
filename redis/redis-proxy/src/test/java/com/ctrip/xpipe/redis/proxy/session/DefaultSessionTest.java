package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Function;

/**
 * @author chen.zhu
 * <p>
 * May 14, 2018
 */
public class DefaultSessionTest extends AbstractRedisProxyServerTest {

    private DefaultSession session;

    @Before
    public void beforeDefaultSessionTest() throws Exception {
        session = (DefaultSession) tunnel().frontend();
    }

    @Test
    public void testForward() throws Exception {
        DefaultSession backend = (DefaultSession) session.tunnel().backend();
        startServer(session.tunnel().getNextJump().getPort(), new Function<String, String>() {
            @Override
            public String apply(String s) {
                logger.info("[receive] {}", s);
                if(s.contains("Proxy")) {
                    return null;
                }
                return "hello";
            }
        });
        protocol().recordPath(session.getChannel());
        backend.connect();
        Thread.sleep(1000);

        session.forward(protocol().output());
        Thread.sleep(1000);
    }

    @Test
    public void testConnect() throws Exception {
        session = (DefaultSession) session.tunnel().backend();
        startServer(session.tunnel().getNextJump().getPort(), new Function<String, String>() {
            @Override
            public String apply(String s) {
                System.out.println("[receive] " + s);
                return "hello";
            }
        });
        session.connect();
    }

    @Test
    public void testTryConnect() {
    }

    @Test
    public void testDisconnect() {
    }

    @Test
    public void testDoDisconnect() {
    }

    @Test
    public void testTryWrite() {
    }

    @Test
    public void testDoWrite() {
    }

    @Test
    public void testGetSessionMeta() {
    }

    @Test
    public void testSetSessionState() {
    }

    @Test
    public void testRelease() {
    }
}