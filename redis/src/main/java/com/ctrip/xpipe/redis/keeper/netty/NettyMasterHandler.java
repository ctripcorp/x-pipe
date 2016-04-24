package com.ctrip.xpipe.redis.keeper.netty;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午3:09:44
 */
public class NettyMasterHandler extends ChannelDuplexHandler{
	
	private static Logger logger = LogManager.getLogger(NettyMasterHandler.class);
	
	public RedisKeeperServer redisKeeperServer;
	
	
	public NettyMasterHandler(RedisKeeperServer redisKeeperServer) {
		
		this.redisKeeperServer =  redisKeeperServer;
	}
	
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		
		if(logger.isInfoEnabled()){
			logger.info("[channelActive]" + ctx.channel());
		}
		
		redisKeeperServer.clientConnected(ctx.channel());
		super.channelActive(ctx);
	}


	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		
		if(logger.isInfoEnabled()){
			logger.info("[channelInactive]" + ctx.channel());
		}

		redisKeeperServer.clientDisConnected(ctx.channel());
		super.channelInactive(ctx);
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		
		super.channelRead(ctx, msg);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		
		logger.error("[exceptionCaught]" + ctx.channel(), cause);
		super.exceptionCaught(ctx, cause);
	}


}
