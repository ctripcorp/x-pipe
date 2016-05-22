package com.ctrip.xpipe.redis.simple.latency;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.Unpooled;
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
	
	private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(4);
	private ScheduledFuture<?> future = null;
	
	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		
		super.channelActive(ctx);
		future = scheduled.scheduleAtFixedRate(new Runnable() {
			
			@Override
			public void run() {
				
				long currentCount = count.incrementAndGet();
				if(currentCount > total){
					future.cancel(false);
					return;
				}
				String data = String.format("set %d %d\r\n", currentCount, System.currentTimeMillis());
				ctx.channel().writeAndFlush(Unpooled.wrappedBuffer(data.getBytes()));
			}
		}, 0, 20, TimeUnit.MICROSECONDS);
	}

}
