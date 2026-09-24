package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.Subscribe;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperPubSubConstants;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;
import com.ctrip.xpipe.redis.keeper.pubsub.KeeperPubSubRegistry;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * SUBSCRIBE：每连接最多 {@link KeeperPubSubConstants#MAX_SUBSCRIBE_CHANNELS} 个 channel（D16 / §4.4.1）。
 */
public class SubscribeCommandHandler extends AbstractCommandHandler {

	static final String ERR_CHANNEL_LIMIT = "subscribe channel limit exceeded";

	@Override
	public String[] getCommands() {
		return new String[]{"subscribe"};
	}

	@Override
	protected void doHandle(String[] args, RedisClient<?> redisClient) {
		logger.debug("[doHandle]{},{}", redisClient, StringUtil.join(" ", args));
		if (args == null || args.length == 0) {
			redisClient.sendMessage(new RedisErrorParser("wrong format").format());
			return;
		}
		KeeperPubSubRegistry registry = registryOf(redisClient);
		if (registry == null) {
			redisClient.sendMessage(new RedisErrorParser("wrong format").format());
			return;
		}
		for (String channel : args) {
			if (!registry.isSubscribed(redisClient, channel)
					&& registry.subscribedChannelCount(redisClient) >= KeeperPubSubConstants.MAX_SUBSCRIBE_CHANNELS) {
				redisClient.sendMessage(new RedisErrorParser(ERR_CHANNEL_LIMIT).format());
				return;
			}
			registry.subscribe(redisClient, channel);
			sendSubscribeAck(redisClient, channel, registry.subscribedChannelCount(redisClient));
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

	private static void sendSubscribeAck(RedisClient<?> redisClient, String channel, long count) {
		redisClient.sendMessage(new ArrayParser(new Object[]{
				new ByteArrayOutputStreamPayload(Subscribe.SUBSCRIBE),
				new ByteArrayOutputStreamPayload(channel),
				count
		}).format());
	}

}
