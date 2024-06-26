package com.ctrip.xpipe.redis.proxy.model;

import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.session.SESSION_TYPE;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.redis.proxy.session.state.SessionInit;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelHalfEstablished;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * May 13, 2018
 */
public class TunnelMetaTest extends AbstractRedisProxyServerTest {

    @Test
    public void testToString() {
        SessionMeta frontend = new SessionMeta(SESSION_TYPE.FRONTEND.name(), 1L, "frontend channel",
                newProxyEndpoint(true, false).getUri(), new SessionEstablished(null).name());
        SessionMeta backend = new SessionMeta(SESSION_TYPE.FRONTEND.name(), 1L, "backend channel",
                newProxyEndpoint(true, false).getUri(), new SessionInit(null).name());
        TunnelMeta meta = new TunnelMeta(new TunnelIdentity(new EmbeddedChannel(), "ABC_DEF", "source"), new TunnelHalfEstablished(null).name(), protocol().getContent(), frontend, backend);

        logger.info("[testToString] {}", meta.toString());
    }
}