package com.ctrip.xpipe.redis.keeper.netty;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.netty.AbstractNettyHandler;
import com.ctrip.xpipe.netty.ByteBufReadAction;
import com.ctrip.xpipe.netty.TrafficReportingEvent;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
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

    private RedisKeeperServer redisKeeperServer;
    
	private RedisMasterReplication redisMasterReplication;
	
	public NettySlaveHandler(RedisMasterReplication redisMasterReplication, RedisKeeperServer redisKeeperServer) {
		this.redisMasterReplication = redisMasterReplication;
		this.redisKeeperServer = redisKeeperServer;
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
	
	@Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof TrafficReportingEvent) {
            TrafficReportingEvent tEvt = (TrafficReportingEvent) evt;
            if(tEvt.getReadBytes() > 0) {
                String type = String.format("Keeper.In.%s", redisKeeperServer.getClusterId());
                String name = redisKeeperServer.getShardId();
                EventMonitor.DEFAULT.logEvent(type, name, tEvt.getReadBytes());
            }
        }
    }
}
