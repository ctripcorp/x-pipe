package com.ctrip.xpipe.netty.commands;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.utils.ThreadUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *
 *         Jul 1, 2016
 */
public class NettyClientFactory extends AbstractStartStoppable implements PooledObjectFactory<NettyClient> {

	private static final AtomicInteger poolId = new AtomicInteger();
	private NioEventLoopGroup eventLoopGroup;
	private final boolean useGlobalResources;
	private Bootstrap b = new Bootstrap();
	private int connectTimeoutMilli = 5000;
	private static Logger logger = LoggerFactory.getLogger(NettyClientFactory.class);
	private Endpoint endpoint;

	public NettyClientFactory(Endpoint endpoint) {
		this(endpoint, true);
	}

	public NettyClientFactory(Endpoint endpoint, boolean useGlobalResources) {
		this.endpoint = endpoint;
		this.useGlobalResources = useGlobalResources;
	}

	@Override
	protected void doStart() throws Exception {
		if (useGlobalResources) {
			eventLoopGroup = NettyClientResource.getGlobalEventLoopGroup();
		} else {
			eventLoopGroup = new NioEventLoopGroup(1, XpipeThreadFactory.create("NettyClientFactory-" + poolId.incrementAndGet()));
		}

		b.group(eventLoopGroup).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
				.handler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(SocketChannel ch) throws Exception {
						ChannelPipeline p = ch.pipeline();
						p.addLast(new LoggingHandler());
						p.addLast(new NettySimpleMessageHandler());
						p.addLast(new NettyClientHandler());
					}
				});
	}

	@Override
	protected void doStop() {
		if (!useGlobalResources) eventLoopGroup.shutdownGracefully();
	}

	@Override
	public PooledObject<NettyClient> makeObject() throws Exception {

		ChannelFuture f = b.connect(endpoint.getHost(), endpoint.getPort());
		f.get(connectTimeoutMilli, TimeUnit.MILLISECONDS);
		Channel channel = f.channel();
		logger.info("[makeObject]{}", channel);
		NettyClient nettyClient = new DefaultNettyClient(channel);
		channel.attr(NettyClientHandler.KEY_CLIENT).set(nettyClient);
		return new DefaultPooledObject<NettyClient>(nettyClient);
	}

	@Override
	public void destroyObject(PooledObject<NettyClient> p) throws Exception {

		logger.info("[destroyObject]{}, {}", endpoint, p.getObject());
		p.getObject().channel().close();

	}

	@Override
	public boolean validateObject(PooledObject<NettyClient> p) {
		return p.getObject().channel().isActive();
	}

	@Override
	public void activateObject(PooledObject<NettyClient> p) throws Exception {

	}

	@Override
	public void passivateObject(PooledObject<NettyClient> p) throws Exception {

	}

	@Override
	public String toString() {
		return String.format("T:%s", endpoint.toString());
	}

	private static final class NettyClientResource {

		private static NioEventLoopGroup globalEventLoopGroup;

		public static NioEventLoopGroup getGlobalEventLoopGroup() {
			if (null != globalEventLoopGroup) {
				return globalEventLoopGroup;
			}

			synchronized(NettyClientResource.class) {
				if (null == globalEventLoopGroup) {
					globalEventLoopGroup = new NioEventLoopGroup(ThreadUtils.bestEffortThreadNums(),
							XpipeThreadFactory.create("NettyClientFactory-Global"));
				}
			}

			return globalEventLoopGroup;
		}

	}

}
