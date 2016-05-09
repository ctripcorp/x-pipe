package com.ctrip.xpipe.redis.keeper.handler;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Map.Entry;

import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.netty.NettySentinelHandler;
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

			if (isValidSlave(redisClient.getRedisKeeperServer(), ip, port)) {
				String msg = String.format("promote %s:%s to master", ip, port);
				redisClient.sendMessage(new BulkStringParser(msg).format());

				promoteSlaveToMaster(ip, port);
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
	private void promoteSlaveToMaster(String ip, int port) {
		waitUntilSlaveSync();
		sendSlaveOfCommand(ip, port);
	}

	/**
	 * 
	 */
   private void waitUntilSlaveSync() {
	   // TODO Auto-generated method stub
	   
   }

	/**
	 * @param ip
	 * @param port
	 */
	private void sendSlaveOfCommand(String ip, int port) {
		// TODO reuse event loop group
		NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();
		Bootstrap b = new Bootstrap();
		b.group(eventLoopGroup).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
		      .handler(new ChannelInitializer<SocketChannel>() {
			      @Override
			      public void initChannel(SocketChannel ch) throws Exception {
				      ChannelPipeline p = ch.pipeline();
				      p.addLast(new NettySimpleMessageHandler());
				      p.addLast(new NettySentinelHandler());
			      }
		      });

		try {
			// TODO connect in separate thread, connect timeout, re-connect
			ChannelFuture f = b.connect(ip, port);
			f.sync();
		} catch (Throwable th) {
			logger.error("[connectSlave][fail]" + port, th);
		}
	}

	/**
	 * @param redisKeeperServer
	 * @param actualIp
	 * @param actualPort
	 * @return
	 */
	private boolean isValidSlave(RedisKeeperServer keeper, String actualIp, int actualPort) {
		Map<Channel, RedisClient> slaves = keeper.slaves();
		for (Entry<Channel, RedisClient> entry : slaves.entrySet()) {
			if (entry.getKey().remoteAddress() instanceof InetSocketAddress) {
				InetSocketAddress slaveAddr = (InetSocketAddress) entry.getKey().remoteAddress();
				String expectedIp = slaveAddr.getAddress().getHostAddress();
				int expectedPort = entry.getValue().getSlaveListeningPort();

				if (expectedIp.equals(actualIp) && expectedPort == actualPort) {
					return true;
				}
			} else {
				// TODO
			}
		}
		return false;
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
