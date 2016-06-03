package com.ctrip.xpipe.redis.keeper.netty;



import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisMaster;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午3:09:44
 */
public class NettySlaveHandler extends AbstractNettyHandler{

	
	private RedisMaster redisMaster;
	
	public NettySlaveHandler(DefaultRedisMaster redisMaster) {
		this.redisMaster = redisMaster;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		
		Channel channel = ctx.channel();

		if(logger.isInfoEnabled()){
			logger.info("[channelActive]" + channel);
		}
		
		redisMaster.masterConnected(channel);
		super.channelActive(ctx);
	}

	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		
		if(logger.isInfoEnabled()){
			logger.info("[channelInactive]" + ctx.channel());
		}
		
		redisMaster.masterDisconntected(ctx.channel());
		super.channelInactive(ctx);
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		
		ByteBuf byteBuf = (ByteBuf) msg;
		byteBufReadPolicy.read(ctx.channel(), byteBuf, new ByteBufReadAction() {
			@Override
			public void read(Channel channel, ByteBuf byteBuf) throws XpipeException {
				redisMaster.handleResponse(channel, byteBuf);
			}
		});
		super.channelRead(ctx, msg);
	}
	
}
