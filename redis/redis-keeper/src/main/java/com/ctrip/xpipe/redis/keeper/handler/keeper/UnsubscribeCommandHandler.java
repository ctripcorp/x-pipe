package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;
import com.ctrip.xpipe.redis.keeper.pubsub.KeeperPubSubRegistry;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.List;

/**
 * UNSUBSCRIBE：订阅方退出的正常路径（D16 / §4.4.1）。无参时退订该连接全部 channel。
 */
public class UnsubscribeCommandHandler extends AbstractCommandHandler {

	static final String ACK_KIND = "unsubscribe";

	@Override
	public String[] getCommands() {
		return new String[]{"unsubscribe"};
	}

	@Override
	protected void doHandle(String[] args, RedisClient<?> redisClient) {
		logger.debug("[doHandle]{},{}", redisClient, StringUtil.join(" ", args));
		KeeperPubSubRegistry registry = registryOf(redisClient);
		if (registry == null) {
			redisClient.sendMessage(new RedisErrorParser("wrong format").format());
			return;
		}
		if (args == null || args.length == 0) {
			List<String> channels = registry.subscribedChannels(redisClient);
			if (channels.isEmpty()) {
				sendUnsubscribeAck(redisClient, "", 0L);
				return;
			}
			for (String channel : channels) {
				registry.unsubscribe(redisClient, channel);
				sendUnsubscribeAck(redisClient, channel, registry.subscribedChannelCount(redisClient));
			}
			return;
		}
		for (String channel : args) {
			registry.unsubscribe(redisClient, channel);
			sendUnsubscribeAck(redisClient, channel, registry.subscribedChannelCount(redisClient));
		}
	}

	@Override
	public boolean isLog(String[] args) {
		return false;
	}

	@Override
	public boolean support(RedisServer server) {
		return server instanceof RedisKeeperServer;
	}

	private static KeeperPubSubRegistry registryOf(RedisClient<?> redisClient) {
		return ((RedisKeeperServer) redisClient.getRedisServer()).getPubSubRegistry();
	}

	private static void sendUnsubscribeAck(RedisClient<?> redisClient, String channel, long remaining) {
		redisClient.sendMessage(new ArrayParser(new Object[]{
				new ByteArrayOutputStreamPayload(ACK_KIND),
				new ByteArrayOutputStreamPayload(channel),
				remaining
		}).format());
	}

}
