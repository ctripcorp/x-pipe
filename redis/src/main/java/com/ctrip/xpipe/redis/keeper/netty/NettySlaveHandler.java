package com.ctrip.xpipe.redis.keeper.netty;



import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisMaster;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午3:09:44
 */
public class NettySlaveHandler extends ChannelDuplexHandler{

	private Logger logger = LoggerFactory.getLogger(NettySlaveHandler.class);
	
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
		
		redisMaster.handleResponse(ctx.channel(), (ByteBuf)msg);
		super.channelRead(ctx, msg);
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

		logger.error("[exceptionCaught]" + ctx.channel(), cause);
		super.exceptionCaught(ctx, cause);
	}

}
