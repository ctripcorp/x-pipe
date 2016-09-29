package com.ctrip.xpipe.netty.commands;



import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.DefaultChannelPromise;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public class DefaultNettyClient implements NettyClient{
	
	private Logger logger = LoggerFactory.getLogger(DefaultNettyClient.class);
	
	private Channel channel;
	private LinkedBlockingQueue<ByteBufReceiver> receivers = new LinkedBlockingQueue<>();
	
	public DefaultNettyClient(Channel channel) {
		this.channel = channel;
		channel.closeFuture().addListener(new ChannelFutureListener() {
			
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				channelClosed(future.channel());
			}
		});
	}

	@Override
	public void sendRequest(ByteBuf byteBuf) {
		channel.writeAndFlush(byteBuf);
	}

	@Override
	public void sendRequest(ByteBuf byteBuf, final ByteBufReceiver byteBufReceiver) {
		
		DefaultChannelPromise future = new DefaultChannelPromise(channel);
		
		future.addListener(new ChannelFutureListener() {
			
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if(future.isSuccess()){
					logger.debug("[operationComplete][add receiver]{}", byteBufReceiver);
					receivers.offer(byteBufReceiver);
				}else{
					logger.error("[sendRequest][fail]" + channel, future.cause());
				}
			}
		});
		channel.writeAndFlush(byteBuf, future);
		
	}

	@Override
	public void handleResponse(Channel channel, ByteBuf byteBuf) {
		
		ByteBufReceiver byteBufReceiver = receivers.peek();
		if(byteBufReceiver != null){
			boolean result = byteBufReceiver.receive(channel, byteBuf);
			if(result){
				logger.debug("[handleResponse][remove receiver]");
				receivers.poll();
			}
		}else{
			logger.error("[handleResponse][no receiver]{}", byteBuf.readableBytes());
		}
	}

	protected void channelClosed(Channel channel) {
		
		logger.info("[channelClosed]{}", channel);
		while(true){
			ByteBufReceiver byteBufReceiver = receivers.poll();
			if(byteBufReceiver == null){
				break;
			}
			byteBufReceiver.clientClosed(this);
		}
	}

	@Override
	public Channel channel() {
		return channel;
	}
	
	@Override
	public String toString() {
		return channel.toString();
	}
}
