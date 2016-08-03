package com.ctrip.xpipe.netty.commands;


import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public class DefaultNettyClient implements NettyClient{
	
	private Logger logger = LoggerFactory.getLogger(DefaultNettyClient.class);
	
	private Channel channel;
	private Queue<ByteBufReceiver> receivers = new ConcurrentLinkedQueue<>();
	
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
		
		ChannelFuture writeFuture = channel.writeAndFlush(byteBuf);
		writeFuture.addListener(new ChannelFutureListener() {
			
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if(future.isSuccess()){
					receivers.offer(byteBufReceiver);
				}
			}
		});
	}

	@Override
	public void handleResponse(Channel channel, ByteBuf byteBuf) {
		
		ByteBufReceiver byteBufReceiver = receivers.peek();
		if(byteBufReceiver != null){
			boolean result = byteBufReceiver.receive(channel, byteBuf);
			if(result){
				receivers.poll();
			}
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
