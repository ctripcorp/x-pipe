package com.ctrip.xpipe.netty.commands;


import com.ctrip.xpipe.netty.AbstractNettyHandler;
import com.ctrip.xpipe.netty.ByteBufReadAction;

import com.ctrip.xpipe.netty.ByteBufReadActionException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public class NettyClientHandler extends AbstractNettyHandler{
	
	public static final AttributeKey<NettyClient> KEY_CLIENT = AttributeKey.newInstance(NettyClientHandler.class.getSimpleName() + "_REDIS_CLIENTS");
	
	public static boolean bind(Channel channel, NettyClient nettyClient){
		
		Attribute<NettyClient> attribute = channel.attr(KEY_CLIENT);
		if(attribute != null){
			return false;
		}
		
		synchronized (channel) {
			attribute = channel.attr(KEY_CLIENT);
			if(attribute == null){
				return false;
			}
			attribute.set(nettyClient);
		}
		return true;
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		
		ByteBuf byteBuf = (ByteBuf) msg;
		final NettyClient nettyClient = ctx.channel().attr(KEY_CLIENT).get();
		
		byteBufReadPolicy.read(ctx.channel(), byteBuf, new ByteBufReadAction() {
			
			@Override
			public void read(Channel channel, ByteBuf byteBuf) throws ByteBufReadActionException {
				nettyClient.handleResponse(channel, byteBuf);
			}
		});
		
		super.channelRead(ctx, msg);
	}

}
