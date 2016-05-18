package com.ctrip.xpipe.redis.keeper.handler;


import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer.KEEPER_STATE;
import com.ctrip.xpipe.redis.keeper.netty.NettyBaseClientHandler;
import com.ctrip.xpipe.redis.protocal.Command;
import com.ctrip.xpipe.redis.protocal.CommandRequester;
import com.ctrip.xpipe.redis.protocal.RequestResponseCommandListener;
import com.ctrip.xpipe.redis.protocal.cmd.Fsync;
import com.ctrip.xpipe.redis.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.protocal.cmd.SlaveOfCommand;
import com.ctrip.xpipe.redis.protocal.protocal.BulkStringParser;
import com.ctrip.xpipe.utils.IpUtils;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
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
 *         May 9, 2016 12:10:38 PM
 */
public class SlaveOfCommandHandler extends AbstractCommandHandler {

	private final static String[] COMMANDS = new String[] { "slaveof" };
	private final static String NO = "no"; 
	private final static String ONE = "one"; 

	/**
	 * slave of ip port
	 */
	@Override
	public String[] getCommands() {
		return COMMANDS;
	}

	@Override
	protected void doHandle(String[] args, RedisClient redisClient) {
		
		String errorMessage = null;
		if (validateArgs(args)) {
			if(args[0].equalsIgnoreCase(NO)){
				final String ip = args[2];
				final int port = Integer.parseInt(args[3]); // already validated
				promotionSlave(redisClient, ip, port);
				return;
			}else{
				errorMessage = "slave connected to keeper, discard command!";
			}
		} else {
			errorMessage = "wrong format";
		}
		
		redisClient.sendMessage(new BulkStringParser(errorMessage).format());
	}

	private void promotionSlave(RedisClient redisClient, final String ip, final int port) {
		
		final RedisClient slave = findSlave(redisClient.getRedisKeeperServer(), ip, port);
		if (slave != null) {
			RedisKeeperServer redisKeeperServer = redisClient.getRedisKeeperServer();

				redisKeeperServer.setKeeperServerState(KEEPER_STATE.BEGIN_PROMOTE_SLAVE);
				String msg = String.format("promote %s:%s to master", ip, port);
				redisClient.sendMessage(new BulkStringParser(msg).format());
				
				new Thread() {
					public void run() {
						promoteSlaveToMaster(slave, ip, port);
					}
				}.start();
		} else {
			String msg = String.format("%s:%s is not a connected slave", ip, port);
			redisClient.sendMessage(new BulkStringParser(msg).format());
		}
		
	}

	/**
	 * @param ip
	 * @param port
	 */
	private void promoteSlaveToMaster(RedisClient redisClient, final String ip, final int port) {
		waitUntilSlaveSync(redisClient, ip, port);
		final RedisKeeperServer keeper = redisClient.getRedisKeeperServer();

		logger.info("fsync start ");
		Fsync fsyncCmd = new Fsync();
		fsyncCmd.setCommandListener(new RequestResponseCommandListener() {

			@Override
			public void onComplete(Channel channel, Object data, Exception e) {
				System.out.println("fsync done " + data);
				SlaveOfCommand slaveOfCmd = new SlaveOfCommand();
				slaveOfCmd.setCommandListener(new RequestResponseCommandListener() {

					@Override
					public void onComplete(Channel channel, Object data, Exception e) {
						if (e == null) {
							InfoCommand infoServerCmd = new InfoCommand("server");
							infoServerCmd.setCommandListener(new RequestResponseCommandListener() {

								@Override
								public void onComplete(Channel channel, Object data, Exception e) {
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
										public void onComplete(Channel channel, Object data, Exception e) {
											String res = new String(((ByteArrayOutputStreamPayload) data).getBytes());
											long keeperOffset = 0, newMasterOffset = 0;
											try {
												String[] parts = res.split("\\s");
												keeperOffset = Long.parseLong(parts[1]);
												newMasterOffset = Long.parseLong(parts[2]);
												
												keeper.setKeeperServerState(KEEPER_STATE.SLAVE_PROMTED, 
														new SlavePromotionInfo(keeperOffset, new DefaultEndPoint(ip, port), 
																masterId.get(), newMasterOffset));
											} catch (Exception ee) {
												e.printStackTrace();
											}
										}
									});

									keeper.getCommandRequester().schedule(TimeUnit.SECONDS, 3, channel, infoLastMasterCmd);
								}
							});
							sendCommandToRedis(ip, port, keeper.getCommandRequester(), infoServerCmd);
						} else {
							// TODO
							logger.error(e);
						}
					}
				});

				sendCommandToRedis(ip, port, keeper.getCommandRequester(), slaveOfCmd);
			}
		});
		sendCommandToRedis(ip, port, keeper.getCommandRequester(), fsyncCmd);

	}

	private void waitUntilSlaveSync(RedisClient redisClient, String ip, int port) {
		Long slaveCmdOffset = redisClient.getAck();
		long masterCmdOffset = redisClient.getRedisKeeperServer().getReplicationStore().endOffset();

		while (slaveCmdOffset == null || slaveCmdOffset != masterCmdOffset) {
			if (logger.isInfoEnabled()) {
				logger.info("wait for slave to sync with keeper before promote to master");
			}
			// TODO timeout
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
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

	/**
	 * @param redisKeeperServer
	 * @param actualIp
	 * @param actualPort
	 * @return
	 */
	private RedisClient findSlave(RedisKeeperServer keeper, String actualIp, int actualPort) {
		Map<Channel, RedisClient> slaves = keeper.slaves();
		for (Entry<Channel, RedisClient> entry : slaves.entrySet()) {
			if (entry.getKey().remoteAddress() instanceof InetSocketAddress) {
				InetSocketAddress slaveAddr = (InetSocketAddress) entry.getKey().remoteAddress();
				String expectedIp = slaveAddr.getAddress().getHostAddress();
				int expectedPort = entry.getValue().getSlaveListeningPort();

				if (expectedIp.equals(actualIp) && expectedPort == actualPort) {
					return entry.getValue();
				}
			} else {
				// TODO
			}
		}
		return null;
	}

	/**
	 * @param args
	 * @return
	 */
	private boolean validateArgs(String[] args) {
		try {
			if (args.length == 4) {
				
				if(args[0].equalsIgnoreCase(NO) || IpUtils.isValidIpFormat(args[0])){
					if(args[1].equalsIgnoreCase(ONE) || IpUtils.isPort(args[1])){
						if (IpUtils.isValidIpFormat(args[2]) && IpUtils.isPort(args[3])) {
							return true;
						}
					}
				}
				
			}
		} catch (Exception e) {
			// ignore
		}
		return false;
	}

	
	public static class SlavePromotionInfo{
		
		private long keeperOffset;
		private Endpoint newMasterEndpoint;
		private String newMasterRunid;
		private long newMasterReplOffset;
		
		public SlavePromotionInfo(long keeperOffset, Endpoint newMasterEndpoint, String newMasterRunid, long newMasterReplOffset){
			this.keeperOffset = keeperOffset;
			this.newMasterEndpoint = newMasterEndpoint;
			this.newMasterRunid = newMasterRunid;
			this.newMasterReplOffset = newMasterReplOffset;
		}
		

		public long getKeeperOffset() {
			return keeperOffset;
		}

		public Endpoint getNewMasterEndpoint() {
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
