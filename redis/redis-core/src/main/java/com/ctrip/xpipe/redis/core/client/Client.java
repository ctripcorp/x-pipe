package com.ctrip.xpipe.redis.core.client;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.redis.core.netty.NettyBaseClientHandler;
import com.ctrip.xpipe.redis.core.protocal.Command;
import com.ctrip.xpipe.redis.core.protocal.CommandRequester;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultCommandRequester;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * @author wenchao.meng
 *
 *         Jun 27, 2016
 */
public class Client extends AbstractLifecycle {
	
	private int reconnnectTimeMilli = 3000;

	private InetSocketAddress address;
	private CommandRequester commandRequester;
	private NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();
	private Bootstrap b = new Bootstrap();
	private ScheduledExecutorService scheduled;
	private Channel channel;

	public Client(InetSocketAddress address, CommandRequester commandRequester, ScheduledExecutorService scheduled) {
		this.address = address;
		this.commandRequester = commandRequester;
		this.scheduled = scheduled;
	}


	public Client(InetSocketAddress address) {
		this.address = address;
		this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("client-" + address));
		this.commandRequester = new DefaultCommandRequester(scheduled);
	}

	@Override
	protected void doInitialize() throws Exception {

		b.group(eventLoopGroup).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
				.handler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(SocketChannel ch) throws Exception {
						ChannelPipeline p = ch.pipeline();
						p.addLast(new NettySimpleMessageHandler());
						p.addLast(new NettyBaseClientHandler(commandRequester));
					}
		});
	}

	@Override
	protected void doStart() throws Exception {
		connect();
	}

	private void connect() throws InterruptedException {
		
		ChannelFuture f = b.connect(address);
		f.sync();
		this.channel = f.channel();
		this.channel.closeFuture().addListener(new ChannelFutureListener() {
			
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				logger.info("[channelClosed]{}", future.channel());
				reconnect();
			}
		});

	}

	public void sendCommand(Command command) {
		commandRequester.request(this.channel, command);
	}

	@Override
	protected void doStop() throws Exception {
		
		if(channel != null && channel.isActive()){
			logger.info("[doStop][close channel]");
			channel.close();
		}
	}
	
	@Override
	protected void doDispose() throws Exception {
		eventLoopGroup.shutdownGracefully();
	}

	public boolean isAlive() {
		return this.channel != null && channel.isActive(); 
	}

	private void reconnect() {
		
		
		if(getLifecycleState().isStopped()){
			logger.info("[reconnect][no reconnect, lifecycle stopped!!]");
			return;
		}
		logger.info("[reconnect][reconnect]{}", address);
		scheduled.schedule(new Runnable() {
			
			@Override
			public void run() {
				try {
					connect();
				} catch (InterruptedException e) {
					logger.error("[run]" + address, e);
				}
			}
		}, reconnnectTimeMilli, TimeUnit.MILLISECONDS);
	}
	
	public int getReconnnectTimeMilli() {
		return reconnnectTimeMilli;
	}
	
	//for unit test
	protected void close(){
		if(channel != null && channel.isActive()){
			channel.close();
		}
	}
}
