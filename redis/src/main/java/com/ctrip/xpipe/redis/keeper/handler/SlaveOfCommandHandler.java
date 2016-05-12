package com.ctrip.xpipe.redis.keeper.handler;


import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.netty.NettyBaseClientHandler;
import com.ctrip.xpipe.redis.protocal.Command;
import com.ctrip.xpipe.redis.protocal.CommandRequester;
import com.ctrip.xpipe.redis.protocal.RequestResponseCommandListener;
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

	/**
	 * slave of ip port
	 */
	@Override
	public String[] getCommands() {
		return COMMANDS;
	}

	@Override
	protected void doHandle(String[] args, RedisClient redisClient) {
		if (validateArgs(args)) {
			String ip = args[0];
			int port = Integer.parseInt(args[1]); // already validated

			RedisClient slave = findSlave(redisClient.getRedisKeeperServer(), ip, port);
			if (slave != null) {
				String msg = String.format("promote %s:%s to master", ip, port);
				redisClient.sendMessage(new BulkStringParser(msg).format());

				promoteSlaveToMaster(slave, ip, port);
			} else {
				String msg = String.format("%s:%s is not a connected slave", ip, port);
				redisClient.sendMessage(new BulkStringParser(msg).format());
			}

		} else {
			redisClient.sendMessage(new BulkStringParser("wrong format").format());
		}
	}

	/**
	 * @param ip
	 * @param port
	 */
	private void promoteSlaveToMaster(RedisClient redisClient, String ip, int port) {
		waitUntilSlaveSync(redisClient, ip, port);
		final RedisKeeperServer keeper = redisClient.getRedisKeeperServer();
		SlaveOfCommand slaveOfCmd = new SlaveOfCommand();
		slaveOfCmd.setCommandListener(new RequestResponseCommandListener() {

			@Override
			public void onComplete(Channel channel, Object data, Exception e) {
				if (e == null) {
					System.out.println(data);
					InfoCommand infoCmd = new InfoCommand();
					infoCmd.setCommandListener(new RequestResponseCommandListener() {

						@Override
						public void onComplete(Channel channel, Object data, Exception e) {
							System.out.println(data);
						}
					});

					keeper.getCommandRequester().schedule(TimeUnit.SECONDS, 3, channel, infoCmd);
				} else {
					// TODO
					logger.error(e);
				}
			}
		});

		sendCommandToRedis(ip, port, keeper.getCommandRequester(), slaveOfCmd);
		// connectToNewMaster(ip, port, masterOffset);
	}

	/**
	 * @param ip
	 * @param port
	 * @param masterOffset
	 */
	@SuppressWarnings("unused")
   private void connectToNewMaster(String ip, int port, long masterOffset) {
		// TODO Auto-generated method stub

	}

	/**
	 * @param keeper
	 * 
	 */
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
			if (args.length == 2) {
				if (IpUtils.isValidIpFormat(args[0])) {
					Integer.parseInt(args[1]);
					return true;
				}
			}
		} catch (Exception e) {
			// ignore
		}

		return false;
	}

}
