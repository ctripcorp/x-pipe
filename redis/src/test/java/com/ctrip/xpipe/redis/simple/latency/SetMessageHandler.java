package com.ctrip.xpipe.redis.simple.latency;



import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author wenchao.meng
 *
 * May 22, 2016 2:45:40 PM
 */
public class SetMessageHandler extends ChannelDuplexHandler{
	
	private AtomicLong count = new AtomicLong();
	
	private final long total = 1 << 30;
	
	private ScheduledFuture<?> future = null;
		
	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		
		super.channelActive(ctx);
		sendMessage(ctx.channel());				
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		
		sendMessage(ctx.channel());
		super.channelRead(ctx, msg);
	}

	private void sendMessage(Channel channel) {
		
		long currentCount = count.incrementAndGet();
		if(currentCount > total){
			future.cancel(false);
			return;
		}
		String data = String.format("set %d %d\r\n", currentCount, System.currentTimeMillis());
		channel.writeAndFlush(Unpooled.wrappedBuffer(data.getBytes()));
	}
}
