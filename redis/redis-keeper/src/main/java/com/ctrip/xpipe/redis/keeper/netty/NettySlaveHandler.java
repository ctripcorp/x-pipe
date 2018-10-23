package com.ctrip.xpipe.redis.keeper.netty;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.netty.ByteBufReadAction;
import com.ctrip.xpipe.netty.ByteBufReadActionException;
import com.ctrip.xpipe.netty.ChannelTrafficStatisticsHandler;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMasterReplication;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午3:09:44
 */
public class NettySlaveHandler extends ChannelTrafficStatisticsHandler{

    private RedisKeeperServer redisKeeperServer;
    
	private RedisMasterReplication redisMasterReplication;
	
	public NettySlaveHandler(RedisMasterReplication redisMasterReplication, RedisKeeperServer redisKeeperServer, long trafficReportIntervalMillis) {
	    super(trafficReportIntervalMillis);
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
	protected void doChannelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

		redisKeeperServer.getKeeperMonitor().getKeeperStats().increaseInputBytes(getReadBytes());
		ByteBuf byteBuf = (ByteBuf) msg;
		byteBufReadPolicy.read(ctx.channel(), byteBuf, new ByteBufReadAction() {
			@Override
			public void read(Channel channel, ByteBuf byteBuf) throws ByteBufReadActionException {
				try {
					redisMasterReplication.handleResponse(channel, byteBuf);
				} catch (XpipeException e) {
					throw new ByteBufReadActionException("handle:" + channel ,e);
				}
			}
		});
	}
	
	@Override
    protected void doReportTraffic(long readBytes, long writtenBytes, String remoteIp, int remotePort) {
		redisKeeperServer.getKeeperMonitor().getKeeperStats().increaseInputBytes(readBytes);
		redisKeeperServer.getKeeperMonitor().getKeeperStats().increaseOutputBytes(writtenBytes);
        if (readBytes > 0) {
            String type = String.format("Keeper.In.%s", redisKeeperServer.getClusterId());
            String name = String.format("%s-%s-%s:%s", redisMasterReplication.redisMaster().roleDesc(), redisKeeperServer.getShardId(), remoteIp, remotePort);
            EventMonitor.DEFAULT.logEvent(type, name, readBytes);
        }
    }

	@Override
    protected void doWrite(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		redisKeeperServer.getKeeperMonitor().getKeeperStats().increaseOutputBytes(getWrittenBytes());
    }
}
