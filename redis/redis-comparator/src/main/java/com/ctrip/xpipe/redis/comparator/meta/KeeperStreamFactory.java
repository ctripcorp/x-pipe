package com.ctrip.xpipe.redis.comparator.meta;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.comparator.compare.CompareLane;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.stream.KeeperReplStream;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 分片任务的建流 / 断流出口。单测注入假实现以断言「未变化不重建」。
 * 同一 {@code shardDbId} 的 N 路共用一条 {@link EventLoop}（D32 ⑤）。
 */
public interface KeeperStreamFactory {

    int DEFAULT_LISTENING_PORT = 8080;

    CompareLane open(long shardDbId, String cluster, String shard, Endpoint endpoint, Runnable dataAvailable);

    void close(CompareLane lane);

    /** 分片任务拆除时释放该 dbId 的 EventLoop 映射；单路 close 不调用。 */
    default void release(long shardDbId) {
    }

    final class Default implements KeeperStreamFactory {

        private final EventLoopGroup group;

        private final ScheduledExecutorService scheduled;

        private final ComparatorConfig config;

        private final int listeningPort;

        private final ConcurrentHashMap<Long, EventLoop> shardLoops = new ConcurrentHashMap<>();

        public Default(EventLoopGroup group, ScheduledExecutorService scheduled, ComparatorConfig config,
                       int listeningPort) {
            this.group = group;
            this.scheduled = scheduled;
            this.config = config;
            if (listeningPort <= 0) {
                throw new IllegalArgumentException("listeningPort must be positive: " + listeningPort);
            }
            this.listeningPort = listeningPort;
        }

        @Override
        public CompareLane open(long shardDbId, String cluster, String shard, Endpoint endpoint,
                                Runnable dataAvailable) {
            EventLoop loop = eventLoopFor(shardDbId);
            KeeperReplStream stream = new KeeperReplStream(endpoint, scheduled, config,
                    dataAvailable, cluster, shard, listeningPort, loop);
            stream.start();
            return stream;
        }

        @VisibleForTesting
        EventLoop eventLoopFor(long shardDbId) {
            return shardLoops.computeIfAbsent(shardDbId, id -> group.next());
        }

        @VisibleForTesting
        int cachedLoopCount() {
            return shardLoops.size();
        }

        @Override
        public void release(long shardDbId) {
            shardLoops.remove(shardDbId);
        }

        @Override
        public void close(CompareLane lane) {
            if (lane instanceof KeeperReplStream) {
                ((KeeperReplStream) lane).stop();
                return;
            }
            if (lane != null) {
                lane.disconnect();
            }
        }
    }
}
