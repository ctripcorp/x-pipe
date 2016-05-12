package com.ctrip.xpipe.redis.keeper.netty;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.protocal.CommandRequester;

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

	private Logger logger = LogManager.getLogger(NettySlaveHandler.class);
	
	private RedisKeeperServer redisKeeperServer;
	
	private CommandRequester commandRequester ;
	
	public NettySlaveHandler(RedisKeeperServer redisKeeperServer, CommandRequester commandRequester) {
		
		this.redisKeeperServer = redisKeeperServer;
		this.commandRequester = commandRequester;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		
		Channel channel = ctx.channel();

		if(logger.isInfoEnabled()){
			logger.info("[channelActive]" + channel);
		}
		
		redisKeeperServer.slaveConnected(channel);
		super.channelActive(ctx);
	}

	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		
		if(logger.isInfoEnabled()){
			logger.info("[channelInactive]" + ctx.channel());
		}
		
		commandRequester.connectionClosed(ctx.channel());
		redisKeeperServer.slaveDisconntected(ctx.channel());
		super.channelInactive(ctx);
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		
		commandRequester.handleResponse(ctx.channel(), (ByteBuf)msg);
		super.channelRead(ctx, msg);
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

		logger.error("[exceptionCaught]" + ctx.channel(), cause);
		super.exceptionCaught(ctx, cause);
	}

}
