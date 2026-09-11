package com.ctrip.xpipe.redis.comparator.meta;

import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.channel.EventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * D32 ⑤：同一 shardDbId 的 N 路共用一条 EventLoop。
 */
public class KeeperStreamFactoryTest {

    @Test
    public void testSameShardDbIdSharesEventLoop() {
        CountingGroup group = new CountingGroup();
        ScheduledExecutorService scheduled = Executors.newSingleThreadScheduledExecutor(
                XpipeThreadFactory.create("factory-sched-test", true));
        try {
            KeeperStreamFactory.Default factory = new KeeperStreamFactory.Default(
                    group, scheduled, new ComparatorConfig(), KeeperStreamFactory.DEFAULT_LISTENING_PORT);
            EventLoop first = factory.eventLoopFor(10L);
            EventLoop again = factory.eventLoopFor(10L);
            EventLoop other = factory.eventLoopFor(20L);
            Assert.assertSame(first, again);
            Assert.assertSame(other, factory.eventLoopFor(20L));
            Assert.assertEquals(2, group.nexts.get());
            Assert.assertEquals(2, factory.cachedLoopCount());

            factory.release(10L);
            Assert.assertEquals(1, factory.cachedLoopCount());
            factory.eventLoopFor(10L);
            Assert.assertEquals(2, factory.cachedLoopCount());
            Assert.assertEquals(3, group.nexts.get());
        } finally {
            scheduled.shutdownNow();
            group.shutdownGracefully();
        }
    }

    static final class CountingGroup extends NioEventLoopGroup {

        final AtomicInteger nexts = new AtomicInteger();

        CountingGroup() {
            super(2, XpipeThreadFactory.create("factory-loop-test", true));
        }

        @Override
        public EventLoop next() {
            nexts.incrementAndGet();
            return super.next();
        }
    }
}
