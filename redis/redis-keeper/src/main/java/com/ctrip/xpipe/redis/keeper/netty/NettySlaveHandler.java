package com.ctrip.xpipe.redis.keeper.netty;




import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.netty.AbstractNettyHandler;
import com.ctrip.xpipe.netty.ByteBufReadAction;
import com.ctrip.xpipe.redis.keeper.RedisMasterReplication;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午3:09:44
 */
public class NettySlaveHandler extends AbstractNettyHandler{

	
	private RedisMasterReplication redisMasterReplication;
	
	public NettySlaveHandler(RedisMasterReplication redisMasterReplication) {
		this.redisMasterReplication = redisMasterReplication;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		
		Channel channel = ctx.channel();

		redisMasterReplication.masterConnected(channel);
		super.channelActive(ctx);
	}

	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		
		if(logger.isInfoEnabled()){
			logger.info("[channelInactive]" + ctx.channel());
		}
		
		redisMasterReplication.masterDisconntected(ctx.channel());
		super.channelInactive(ctx);
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		
		ByteBuf byteBuf = (ByteBuf) msg;
		byteBufReadPolicy.read(ctx.channel(), byteBuf, new ByteBufReadAction() {
			@Override
			public void read(Channel channel, ByteBuf byteBuf) throws XpipeException {
				redisMasterReplication.handleResponse(channel, byteBuf);
			}
		});
		super.channelRead(ctx, msg);
	}
	
}
