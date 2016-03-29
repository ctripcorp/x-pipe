package com.ctrip.xpipe.api.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午2:11:32
 */
public class SimpleByteHandler extends ChannelDuplexHandler{
	
	private ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		super.channelRead(ctx, msg);
	}
	
	
	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		
		
		ByteBuf byteBuf = null;
		if(msg instanceof byte[]){
			byte[] data = (byte[])msg;
			byteBuf = allocator.buffer(data.length);
			byteBuf.writeBytes(data);
		}else if(msg instanceof ByteBuf){
			byteBuf =  (ByteBuf) msg; 
		}else{
			throw new IllegalArgumentException("unknown parameter type:" + msg);
		}
		super.write(ctx, byteBuf, promise);
	}

}
