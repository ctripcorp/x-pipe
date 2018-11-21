package com.ctrip.xpipe.redis.proxy.monitor.stats;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.monitor.stats.impl.DefaultSocketStats;
import io.netty.channel.Channel;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public class DefaultSocketStatsTest extends AbstractTest {

    @Test
    public void testGetSocketStats() {
        Session session = mock(Session.class);
        Channel channel = mock(Channel.class);
        when(session.getChannel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 6379));
        when(channel.localAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 6389));
        SocketStats socketStats = new DefaultSocketStats(scheduled, session);
        socketStats.getSocketStatsResult();
        sleep(1000);
        socketStats.getSocketStatsResult();
    }
}