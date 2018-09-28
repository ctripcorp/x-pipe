package com.ctrip.xpipe.netty;

import com.ctrip.xpipe.api.codec.Codec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.nio.charset.Charset;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午2:57:59
 */
public class NettySimpleMessageHandler extends ChannelDuplexHandler{
	
	private Charset charset = Codec.defaultCharset;
	
	public NettySimpleMessageHandler() {
		
	}
	public NettySimpleMessageHandler(Charset charset) {
		this.charset = charset;
	}

	
	
	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		
		ByteBufAllocator allocator = ctx.alloc();
		
		byte []data = null;
		
		if(msg instanceof String){
			data = ((String)msg).getBytes(charset); 
		}else if(msg instanceof byte[]){
			data = (byte[])msg;
		}
		
		if(data != null){
			ByteBuf byteBuf = allocator.buffer(data.length);
			byteBuf.writeBytes(data);
			super.write(ctx, byteBuf, promise);
			return;
		}
		
		super.write(ctx, msg, promise);
	}
}
