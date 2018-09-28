package com.ctrip.xpipe.netty;

import com.ctrip.xpipe.utils.ChannelUtil;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * Jun 2, 2016
 */
public abstract class AbstractNettyHandler extends ChannelDuplexHandler{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected ByteBufReadPolicy byteBufReadPolicy = new RetryByteBufReadPolicy();

	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		
		logger.info("[channelActive]{}", ChannelUtil.getDesc(ctx.channel()));
		
		super.channelActive(ctx);
	}


	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		
		logger.error("[exceptionCaught][close channel]" + ChannelUtil.getDesc(ctx.channel()), cause);
		ctx.channel().close();
		
		super.exceptionCaught(ctx, cause);
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {

		logger.info("[channelInactive]{}", ChannelUtil.getDesc(ctx.channel()));

		super.channelInactive(ctx);
	}

}
