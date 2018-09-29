package com.ctrip.xpipe.netty.commands;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
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

/**
 * @author wenchao.meng
 *
 *         Jul 1, 2016
 */
public class NettyClientFactory extends AbstractStartStoppable implements PooledObjectFactory<NettyClient> {

	private NioEventLoopGroup eventLoopGroup;
	private Bootstrap b = new Bootstrap();
	private int connectTimeoutMilli = 5000;
	private static Logger logger = LoggerFactory.getLogger(NettyClientFactory.class);
	private Endpoint endpoint;

	public NettyClientFactory(Endpoint endpoint) {
		this.endpoint = endpoint;
	}

	@Override
	protected void doStart() throws Exception {
		
		eventLoopGroup = new NioEventLoopGroup(1);
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
		eventLoopGroup.shutdownGracefully();
	}

	@Override
	public PooledObject<NettyClient> makeObject() throws Exception {

		ChannelFuture f = b.connect(endpoint.getHost(), endpoint.getPort());
		f.get(connectTimeoutMilli, TimeUnit.MILLISECONDS);
		Channel channel = f.channel();
		sendProxyProtocolIfNeeded(channel);
		logger.info("[makeObject]{}", channel);
		NettyClient nettyClient = new DefaultNettyClient(channel);
		channel.attr(NettyClientHandler.KEY_CLIENT).set(nettyClient);
		return new DefaultPooledObject<NettyClient>(nettyClient);
	}

	private void sendProxyProtocolIfNeeded(Channel channel) {
		if(endpoint instanceof ProxyEnabled) {
			channel.writeAndFlush(((ProxyEnabled) endpoint).getProxyProtocol());
		}
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

}
