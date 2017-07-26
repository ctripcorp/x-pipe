package com.ctrip.xpipe.netty.commands;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LoggingHandler;

/**
 * @author wenchao.meng
 *
 *         Jul 1, 2016
 */
public class NettyKeyedPoolClientFactory extends AbstractStartStoppable implements KeyedPooledObjectFactory<InetSocketAddress, NettyClient> {

	public static final int DEFAULT_KEYED_POOLED_CLIENT_FACTORY_EVNET_LOOP_THREAD = Integer.parseInt(System.getProperty("KEYED_POOLED_CLIENT_FACTORY_EVNET_LOOP_THREAD", "8"));
	private int eventLoopThreads;
	private NioEventLoopGroup eventLoopGroup;
	private Bootstrap b = new Bootstrap();
	private int connectTimeoutMilli = 5000;
	private static Logger logger = LoggerFactory.getLogger(NettyKeyedPoolClientFactory.class);

	public NettyKeyedPoolClientFactory() {
		this(DEFAULT_KEYED_POOLED_CLIENT_FACTORY_EVNET_LOOP_THREAD);
	}

	public NettyKeyedPoolClientFactory(int eventLoopThreads) {
		this.eventLoopThreads = eventLoopThreads;

	}
	
	@Override
	protected void doStart() throws Exception {
		
		eventLoopGroup = new NioEventLoopGroup(eventLoopThreads, XpipeThreadFactory.create("NettyKeyedPoolClientFactory"));
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
	public PooledObject<NettyClient> makeObject(InetSocketAddress key) throws Exception {

		ChannelFuture f = b.connect(key);
		f.get(connectTimeoutMilli, TimeUnit.MILLISECONDS);
		Channel channel = f.channel();
		logger.debug("[makeObject]{}", channel);
		NettyClient nettyClient = new DefaultNettyClient(channel);
		channel.attr(NettyClientHandler.KEY_CLIENT).set(nettyClient);
		return new DefaultPooledObject<NettyClient>(nettyClient);
	}

	@Override
	public void destroyObject(InetSocketAddress key, PooledObject<NettyClient> p) throws Exception {

		logger.info("[destroyObject]{}, {}", key, p.getObject());
		p.getObject().channel().close();

	}

	@Override
	public boolean validateObject(InetSocketAddress key, PooledObject<NettyClient> p) {
		return p.getObject().channel().isActive();
	}

	@Override
	public void activateObject(InetSocketAddress key, PooledObject<NettyClient> p) throws Exception {

	}

	@Override
	public void passivateObject(InetSocketAddress key, PooledObject<NettyClient> p) throws Exception {

	}


	@Override
	protected void doStop() {
		eventLoopGroup.shutdownGracefully();
	}
}
