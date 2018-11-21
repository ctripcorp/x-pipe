package com.ctrip.xpipe.netty.commands;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 *         Jul 1, 2016
 */
public class NettyKeyedPoolClientFactory extends AbstractStartStoppable implements KeyedPooledObjectFactory<Endpoint, NettyClient> {

	public static final int DEFAULT_KEYED_POOLED_CLIENT_FACTORY_EVNET_LOOP_THREAD = Integer.parseInt(System.getProperty("KEYED_POOLED_CLIENT_FACTORY_EVNET_LOOP_THREAD", "8"));
	private int eventLoopThreads;
	protected NioEventLoopGroup eventLoopGroup;
	protected Bootstrap b = new Bootstrap();
	protected int connectTimeoutMilli = 2000;
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
		initBootstrap();
	}

	protected void initBootstrap() {
		b.group(eventLoopGroup).channel(NioSocketChannel.class)
				.option(ChannelOption.TCP_NODELAY, true)
				.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMilli)
				.handler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(SocketChannel ch) {
						ChannelPipeline p = ch.pipeline();
						p.addLast(new LoggingHandler());
						p.addLast(new NettySimpleMessageHandler());
						p.addLast(new NettyClientHandler());
					}
				});
	}

	@Override
	public PooledObject<NettyClient> makeObject(Endpoint key) throws Exception {

		ChannelFuture f = b.connect(key.getHost(), key.getPort());
		NettyClient nettyClient = new AsyncNettyClient(f, key);
		f.channel().attr(NettyClientHandler.KEY_CLIENT).set(nettyClient);
		return new DefaultPooledObject<NettyClient>(nettyClient);
	}

	@Override
	public void destroyObject(Endpoint key, PooledObject<NettyClient> p) throws Exception {

		logger.info("[destroyObject]{}, {}", key, p.getObject());
		p.getObject().channel().close();

	}

	@Override
	public boolean validateObject(Endpoint key, PooledObject<NettyClient> p) {
		Channel channel = p.getObject().channel();
		if(channel == null) {
			return false;
		}
		return channel.isOpen();
	}

	@Override
	public void activateObject(Endpoint key, PooledObject<NettyClient> p) throws Exception {

	}

	@Override
	public void passivateObject(Endpoint key, PooledObject<NettyClient> p) throws Exception {

	}


	@Override
	protected void doStop() {
		eventLoopGroup.shutdownGracefully();
	}
}
