package com.ctrip.xpipe.redis.keeper.netty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author wenchao.meng
 *
 * Jun 2, 2016
 */
public class AbstractNettyHandler extends ChannelDuplexHandler{
	
	protected Logger logger = LoggerFactory.getLogger(NettySlaveHandler.class);
	
	protected ByteBufReadPolicy byteBufReadPolicy = new RetryByteBufReadPolicy();

	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		if(logger.isInfoEnabled()){
			logger.info("[channelActive]" + ctx.channel());
		}
		
		super.channelActive(ctx);
	}


	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		
		logger.error("[exceptionCaught][close channel]" + ctx.channel(), cause);
		ctx.channel().close();
		
		super.exceptionCaught(ctx, cause);
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {

		if(logger.isInfoEnabled()){
			logger.info("[channelInactive]" + ctx.channel());
		}

		super.channelInactive(ctx);
	}

}
