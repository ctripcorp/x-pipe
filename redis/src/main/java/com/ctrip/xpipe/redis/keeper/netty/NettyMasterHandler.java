package com.ctrip.xpipe.redis.keeper.netty;


import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.handler.CommandHandlerManager;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午3:09:44
 */
public class NettyMasterHandler extends ChannelDuplexHandler implements Observer{
	
	private static Logger logger = LoggerFactory.getLogger(NettyMasterHandler.class);
	
	private RedisKeeperServer redisKeeperServer;
	
	private CommandHandlerManager commandHandlerManager;
	
	private static final AttributeKey<RedisClient> KEY_CLIENT = AttributeKey.newInstance(NettyMasterHandler.class.getSimpleName() + "_REDIS_CLIENTS");
	
	public NettyMasterHandler(RedisKeeperServer redisKeeperServer, CommandHandlerManager commandHandlerManager) {
		
		this.redisKeeperServer =  redisKeeperServer;
		this.commandHandlerManager = commandHandlerManager;
	}
	
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		
		if(logger.isInfoEnabled()){
			logger.info("[channelActive]" + ctx.channel());
		}

		Channel channel = ctx.channel();
		RedisClient redisClient = redisKeeperServer.clientConnected(ctx.channel());
		redisClient.addObserver(this);
		getChannelRedisClient(channel).set(redisClient);
		super.channelActive(ctx);
	}

	private Attribute<RedisClient> getChannelRedisClient(Channel channel) {
		
		Attribute<RedisClient> client = channel.attr(KEY_CLIENT);
		return client;
		
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
		
		if(logger.isDebugEnabled()){
			logger.debug(String.format("0X%X, %s", msg.hashCode(), msg.getClass()));
		}
		RedisClient redisClient = getChannelRedisClient(ctx.channel()).get();
		String []args= redisClient.readCommands((ByteBuf)msg);
		if(args != null){
			commandHandlerManager.handle(args, redisClient);;
		}
		super.channelRead(ctx, msg);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		
		logger.error("[exceptionCaught][close channel]" + ctx.channel(), cause);
		ctx.channel().close();
		
		super.exceptionCaught(ctx, cause);
	}

	@Override
	public void update(Object args, Observable observable) {
		
		if(args instanceof RedisSlave){
			logger.info("[update][become redis slave]" + args);
			Attribute<RedisClient> client = getChannelRedisClient(((RedisClient)observable).channel());
			client.set((RedisSlave)args);
		}
	}
}
