package com.ctrip.xpipe.redis.comparator.meta;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientHandler;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConstants;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigGetCommand;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeper 级 {@code CONFIG GET prepare-watch} 缓存（D35 ① / §4.6.2）。
 * 与 Meta 同周期 {@link #invalidateAll()}；查询异步，禁止在 Scheduled 上同步连 Keeper。
 */
public interface PrepareWatchCache {

    void invalidateAll();

    CommandFuture<Boolean> query(Endpoint endpoint);

    final class Default implements PrepareWatchCache {

        private static final Logger logger = LoggerFactory.getLogger(Default.class);

        private final EventLoopGroup group;

        private final ScheduledExecutorService scheduled;

        private final ConcurrentHashMap<String, Boolean> cache = new ConcurrentHashMap<>();

        private final ConcurrentHashMap<String, CommandFuture<Boolean>> inflight = new ConcurrentHashMap<>();

        private final AtomicInteger generation = new AtomicInteger();

        public Default(EventLoopGroup group, ScheduledExecutorService scheduled) {
            this.group = group;
            this.scheduled = scheduled;
        }

        @Override
        public void invalidateAll() {
            generation.incrementAndGet();
            cache.clear();
            inflight.clear();
        }

        @Override
        public CommandFuture<Boolean> query(Endpoint endpoint) {
            if (endpoint == null || StringUtil.isEmpty(endpoint.getHost()) || endpoint.getPort() <= 0) {
                DefaultCommandFuture<Boolean> failed = new DefaultCommandFuture<>();
                failed.setFailure(new IllegalArgumentException("invalid endpoint"));
                return failed;
            }
            String key = endpoint.getHost() + ":" + endpoint.getPort();
            Boolean hit = cache.get(key);
            if (hit != null) {
                DefaultCommandFuture<Boolean> done = new DefaultCommandFuture<>();
                done.setSuccess(hit);
                return done;
            }
            int gen = generation.get();
            return inflight.computeIfAbsent(key, k -> launch(endpoint, k, gen));
        }

        private CommandFuture<Boolean> launch(Endpoint endpoint, String key, int gen) {
            DefaultCommandFuture<Boolean> done = new DefaultCommandFuture<>();
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group.next())
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, ComparatorConstants.STREAM_CONNECT_TIMEOUT_MILLI)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new NettySimpleMessageHandler());
                            ch.pipeline().addLast(new NettyClientHandler());
                        }
                    });
            bootstrap.connect(endpoint.getHost(), endpoint.getPort()).addListener((ChannelFuture future) -> {
                if (!future.isSuccess() || !future.channel().isActive()) {
                    future.channel().close();
                    complete(done, key, false, future.cause() == null
                            ? new IllegalStateException("connect fail " + key) : future.cause());
                    return;
                }
                NettyClient client = new DefaultNettyClient(future.channel());
                future.channel().attr(NettyClientHandler.KEY_CLIENT).set(client);
                ConfigGetCommand.ConfigGetPrepareWatch cmd = new ConfigGetCommand.ConfigGetPrepareWatch(
                        new FixedObjectPool<>(client), scheduled);
                cmd.setCommandTimeoutMilli(ComparatorConstants.STREAM_CONNECT_TIMEOUT_MILLI);
                cmd.execute().addListener(watch -> {
                    try {
                        if (!watch.isSuccess()) {
                            complete(done, key, false, watch.cause());
                            return;
                        }
                        boolean enabled = Boolean.TRUE.equals(watch.getNow());
                        if (gen == generation.get()) {
                            cache.put(key, enabled);
                        }
                        complete(done, key, enabled, null);
                    } catch (Throwable t) {
                        complete(done, key, false, t);
                    } finally {
                        future.channel().close();
                    }
                });
            });
            done.addListener(f -> inflight.remove(key, done));
            return done;
        }

        private void complete(DefaultCommandFuture<Boolean> done, String key, boolean enabled, Throwable cause) {
            if (done.isDone()) {
                return;
            }
            if (cause != null) {
                logger.warn("[prepareWatch] query fail keeper={}", key, cause);
                done.setFailure(cause);
                return;
            }
            done.setSuccess(enabled);
        }
    }
}
