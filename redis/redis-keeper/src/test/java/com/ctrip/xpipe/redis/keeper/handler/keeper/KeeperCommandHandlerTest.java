package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import org.junit.Test;

import static com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser.WHITE_SPACE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * May 23, 2018
 */
public class KeeperCommandHandlerTest {

    private KeeperCommandHandler handler = new KeeperCommandHandler();

    @Test
    public void testGetProxyProtocol() {
        final String command = "setstate ACTIVE 127.0.0.1 6379 PROXY ROUTE PROXYTCP://127.0.0.1:80,PROXYTCP://10.2.1.1:80 TCP";
        ProxyConnectProtocol protocol = handler.getProxyProtocol(command.split(WHITE_SPACE));
        System.out.println(protocol.getContent());
    }

    @Test
    public void testSetindexOff() throws Exception {

        String message = null;
        try {
            handler.doHandle("setindex OFF".split(WHITE_SPACE), null);
        } catch (Throwable t) {
            message = t.getMessage();
        }
        assertEquals("setstate OFF not supported", message);
    }

    @Test
    public void testSetindexABC() {

        String message = null;
        try {
            handler.doHandle("setindex ABC".split(WHITE_SPACE), null);
        } catch (Throwable t) {
            message = t.getMessage();
        }
        assertEquals("No enum constant com.ctrip.xpipe.redis.core.meta.KeeperIndexState.ABC", message);
    }

    @Test
    public void testUnknownCommand() {

        String message = null;
        try {
            handler.doHandle("abc ABC".split(WHITE_SPACE), null);
        } catch (Throwable t) {
            message = t.getMessage();
        }
        assertEquals("unknown command:abc", message);
    }
}