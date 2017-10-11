package com.ctrip.xpipe.redis.keeper.simple.latency.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 * May 22, 2016 2:57:02 PM
 */
public class ReceiveMessageHandler extends ChannelDuplexHandler{

	private Logger logger = LoggerFactory.getLogger(getClass());
	private String runId;
	private long startOffset;
	private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(4);
	
	public ReceiveMessageHandler(String runId, long startOffset) {
		this.runId = runId;
		this.startOffset = startOffset;
		
	}
	
	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		
		super.channelActive(ctx);
		
		ctx.channel().writeAndFlush(Unpooled.wrappedBuffer(String.format("psync %s %d\r\n", runId, startOffset).getBytes()));
		scheduled.scheduleAtFixedRate(new Runnable() {
			
			@Override
			public void run() {
				try{
					ctx.channel().writeAndFlush(Unpooled.wrappedBuffer(String.format("replconf ack 0\r\n", runId, startOffset).getBytes()));
				}catch(Exception e){
					logger.error("[run]", e);
				}
			}
		}, 5, 1, TimeUnit.SECONDS);
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		
		ByteBuf source = (ByteBuf)msg;
		ByteBuf dst = ByteBufAllocator.DEFAULT.heapBuffer(source.readableBytes());
		source.readBytes(dst);
		
		super.channelRead(ctx, msg);
	}

}
