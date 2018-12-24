package com.ctrip.xpipe.netty.commands;

import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.utils.ChannelUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.DefaultChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public class DefaultNettyClient implements NettyClient{
	
	private Logger logger = LoggerFactory.getLogger(DefaultNettyClient.class);
	
	protected Channel channel;
	protected final AtomicReference<String> desc = new AtomicReference<>();
	protected Queue<ByteBufReceiver> receivers = new ConcurrentLinkedQueue<>();
	
	public DefaultNettyClient(Channel channel) {
		this.channel = channel;
		this.desc.set(ChannelUtil.getDesc(channel));
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

		logger.debug("[sendRequest][begin]{}, {}", byteBufReceiver, this);

		DefaultChannelPromise future = new DefaultChannelPromise(channel);
		
		future.addListener(new ChannelFutureListener() {
			
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {

				if(future.isSuccess()){
					logger.debug("[operationComplete][add receiver]{}, {}", byteBufReceiver, this);
					receivers.offer(byteBufReceiver);
				}else{
					logger.error("[sendRequest][fail]" + channel, future.cause());
				}
			}
		});

		logger.debug("[sendRequest][ end ]{}, {}", byteBufReceiver, this);
		channel.writeAndFlush(byteBuf, future);
	}

	@Override
	public void handleResponse(Channel channel, ByteBuf byteBuf) {
		
		ByteBufReceiver byteBufReceiver = receivers.peek();

		if(byteBufReceiver != null){

			ByteBufReceiver.RECEIVER_RESULT result = byteBufReceiver.receive(channel, byteBuf);
			switch (result){
				case SUCCESS:
					logger.debug("[handleResponse][remove receiver]");
					receivers.poll();
					break;
				case CONTINUE:
					//nothing need to be done
					break;
				case FAIL:
					logger.info("[handleResponse][fail, close channel]{}, {}", byteBufReceiver, channel);
					channel.close();
					break;
				case ALREADY_FINISH:
					logger.info("[handleResponse][already finish, close channel]{}, {}", byteBufReceiver, channel);
					channel.close();
					break;
				default:
					throw new IllegalStateException("unknown result:" + result);
			}
		}else{
			logger.error("[handleResponse][no receiver][close client]{}, {}, {}", channel, byteBuf.readableBytes(), ByteBufUtils.readToString(byteBuf));
			channel.close();
		}
	}

	protected void channelClosed(Channel channel) {
		
		logger.info("[channelClosed]{}", channel);
		while(!receivers.isEmpty()){
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
		return desc.get();
	}
}
