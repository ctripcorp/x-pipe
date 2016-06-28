package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.netty.NettyBaseClientHandler;
import com.ctrip.xpipe.redis.core.protocal.CmdContext;
import com.ctrip.xpipe.redis.core.protocal.Command;
import com.ctrip.xpipe.redis.core.protocal.CommandRequester;
import com.ctrip.xpipe.redis.core.protocal.RequestResponseCommandListener;
import com.ctrip.xpipe.redis.core.protocal.cmd.Fsync;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.exception.RedisSlavePromotionException;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer.PROMOTION_STATE;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * @author marsqing
 *
 * Jun 22, 2016
 */
public class RedisPromotor {
	
	protected Logger logger = LoggerFactory.getLogger(RedisPromotor.class);
	
	private final RedisKeeperServer redisKeeperServer;
	private final String promoteServerIp;
	private final int promoteServerPort;
	
	public RedisPromotor(RedisKeeperServer redisKeeperServer, String promoteServerIp, int promoteServerPort){
		
		this.redisKeeperServer = redisKeeperServer;
		this.promoteServerIp = promoteServerIp;
		this.promoteServerPort = promoteServerPort;
	}
	
	public void promote() throws RedisSlavePromotionException{
		

		final RedisSlave redisSlave = findSlave(this.redisKeeperServer, this.promoteServerIp, this.promoteServerPort);
		if(redisSlave == null){
			String msg = String.format("%s:%s is not a connected slave", promoteServerIp, promoteServerPort);
			throw new RedisSlavePromotionException(msg);
		}
		
		logger.info("[promote]{},{} ,{}:{}", redisKeeperServer, redisSlave, promoteServerIp, promoteServerPort);
		redisKeeperServer.getRedisKeeperServerState().setPromotionState(PROMOTION_STATE.BEGIN_PROMOTE_SLAVE);
		new Thread() {
			public void run() {
				promoteSlaveToMaster(redisSlave);
			}
		}.start();
	}

	private void promoteSlaveToMaster(RedisSlave redisSlave) {
		
		waitUntilSlaveSync(redisSlave, this.promoteServerIp, this.promoteServerPort);
		logger.info("[promoteSlaveToMaster][fsync start]{},{}", this.promoteServerIp, this.promoteServerPort);
		
		Fsync fsyncCmd = new Fsync();
		fsyncCmd.setCommandListener(new RequestResponseCommandListener() {

			@Override
			public void onComplete(CmdContext cmdContext, Object data, Exception e) {
				
				logger.info("[promoteSlaveToMaster][fsync done]{},{}", promoteServerIp, promoteServerPort);
				SlaveOfCommand slaveOfCmd = new SlaveOfCommand();
				slaveOfCmd.setCommandListener(new RequestResponseCommandListener() {

					@Override
					public void onComplete(CmdContext cmdContext, Object data, Exception e) {
						if (e == null) {
							InfoCommand infoServerCmd = new InfoCommand("server");
							infoServerCmd.setCommandListener(new RequestResponseCommandListener() {

								@Override
								public void onComplete(CmdContext cmdContext, Object data, Exception e) {
									String res = new String(((ByteArrayOutputStreamPayload) data).getBytes());
									List<String> lines = null;
									try {
										lines = IOUtils.readLines(new StringReader(res));
									} catch (IOException e1) {
										// TODO
									}

									final AtomicReference<String> masterId = new AtomicReference<String>();
									for (String line : lines) {
										if (line.startsWith("run_id:")) {
											masterId.set(line.substring("run_id:".length()));
										}
									}

									InfoCommand infoLastMasterCmd = new InfoCommand("lastmaster");
									infoLastMasterCmd.setCommandListener(new RequestResponseCommandListener() {

										@Override
										public void onComplete(CmdContext cmdContext, Object data, Exception e) {
											String res = new String(((ByteArrayOutputStreamPayload) data).getBytes());
											long keeperOffset = 0, newMasterOffset = 0;
											try {
												String[] parts = res.split("\\s");
												keeperOffset = Long.parseLong(parts[1]);
												newMasterOffset = Long.parseLong(parts[2]);
												
												redisKeeperServer.getRedisKeeperServerState().setPromotionState(
														PROMOTION_STATE.SLAVE_PROMTED, new SlavePromotionInfo(keeperOffset, new DefaultEndPoint(promoteServerIp, promoteServerPort), 
																masterId.get(), newMasterOffset));
											} catch (Exception ee) {
												logger.error("[onComplete]" + promoteServerIp + ":" + promoteServerPort, e);
											}
										}
									});
									cmdContext.schedule(TimeUnit.SECONDS, 3, infoLastMasterCmd);
								}
							});
							cmdContext.sendCommand(infoServerCmd);
						} else {
							logger.error("slaveof command error." + promoteServerIp + ":" + promoteServerPort, e);
						}
					}
				});

				sendCommandToRedis(promoteServerIp, promoteServerPort, redisKeeperServer.getCommandRequester(), slaveOfCmd);
			}
		});
		sendCommandToRedis(promoteServerIp, promoteServerPort, redisKeeperServer.getCommandRequester(), fsyncCmd);
	}

	private void waitUntilSlaveSync(RedisSlave redisSlave, String ip, int port) {
		

		while (true) {

			Long slaveCmdOffset = redisSlave.getAck();
			long masterCmdOffset = redisSlave.getRedisKeeperServer().getKeeperRepl().getEndOffset();
			
			if(slaveCmdOffset == null || slaveCmdOffset < masterCmdOffset){
				if (logger.isInfoEnabled()) {
					logger.info("[waitUntilSlaveSync]{}, {} < {}", redisSlave, slaveCmdOffset, masterCmdOffset);
				}
				// TODO timeout
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
			}else{
				break;
			}
		}
	}

	/**
	 * @param keeper
	 * @param ip
	 * @param port
	 * @param commandRequester
	 */
	private void sendCommandToRedis(String ip, int port, final CommandRequester cmdRequester, final Command cmd) {
		// TODO reuse event loop group
		// TODO close channel, use channel manager?
		NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();
		Bootstrap b = new Bootstrap();
		b.group(eventLoopGroup).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
		      .handler(new ChannelInitializer<SocketChannel>() {
			      @Override
			      public void initChannel(SocketChannel ch) throws Exception {
				      ChannelPipeline p = ch.pipeline();
				      p.addLast(new NettySimpleMessageHandler());
				      p.addLast(new NettyBaseClientHandler(cmdRequester, cmd));
			      }
		      });

		try {
			// TODO connect in separate thread, connect timeout, re-connect
			ChannelFuture f = b.connect(ip, port);
			f.sync();
		} catch (Throwable th) {
			logger.error("[connectRedis][fail]" + port, th);
		}
	}

	private RedisSlave findSlave(RedisKeeperServer keeper, String actualIp, int actualPort) {
		Set<RedisSlave> slaves = keeper.slaves();
		for (RedisSlave redisSlave : slaves) {
			
			InetSocketAddress slaveAddr = (InetSocketAddress) redisSlave.channel().remoteAddress();
			String expectedIp = slaveAddr.getAddress().getHostAddress();
			int expectedPort = redisSlave.getSlaveListeningPort();

			if (expectedIp.equals(actualIp) && expectedPort == actualPort) {
				return redisSlave;
			}
		}
		return null;
	}

	
	public static class SlavePromotionInfo{
		
		private long keeperOffset;
		private DefaultEndPoint newMasterEndpoint;
		private String newMasterRunid;
		private long newMasterReplOffset;
		
		public SlavePromotionInfo(long keeperOffset, DefaultEndPoint newMasterEndpoint, String newMasterRunid, long newMasterReplOffset){
			this.keeperOffset = keeperOffset;
			this.newMasterEndpoint = newMasterEndpoint;
			this.newMasterRunid = newMasterRunid;
			this.newMasterReplOffset = newMasterReplOffset;
		}
		

		public long getKeeperOffset() {
			return keeperOffset;
		}

		public DefaultEndPoint getNewMasterEndpoint() {
			return newMasterEndpoint;
		}

		public String getNewMasterRunid() {
			return newMasterRunid;
		}

		public long getNewMasterReplOffset() {
			return newMasterReplOffset;
		}
		
		@Override
		public String toString() {
			return String.format(
					"keeperOffset:%d, newMasterEndpoint:%s, newMasterRunid:%s, newMasterReplOffset:%d",
					keeperOffset, newMasterEndpoint.toString(), newMasterRunid, newMasterReplOffset
					);
		}
	}
}
