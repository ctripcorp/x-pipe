package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.redis.keeper.handler.keeper.KeeperCommandHandler;
import org.junit.Test;

import static com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser.WHITE_SPACE;

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
}