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
 * PSUBSCRIBE：只接受 {@code *}（D16 / §4.4.1）。
 */
public class PsubscribeCommandHandler extends AbstractCommandHandler {

	static final String ERR_PATTERN = "psubscribe only supports *";

	@Override
	public String[] getCommands() {
		return new String[]{"psubscribe"};
	}

	@Override
	protected void doHandle(String[] args, RedisClient<?> redisClient) {
		logger.debug("[doHandle]{},{}", redisClient, StringUtil.join(" ", args));
		if (args == null || args.length != 1
				|| !KeeperPubSubConstants.PSUBSCRIBE_PATTERN.equals(args[0])) {
			redisClient.sendMessage(new RedisErrorParser(ERR_PATTERN).format());
			return;
		}
		KeeperPubSubRegistry registry = registryOf(redisClient);
		if (registry == null) {
			redisClient.sendMessage(new RedisErrorParser("wrong format").format());
			return;
		}
		registry.psubscribeAll(redisClient);
		redisClient.sendMessage(new ArrayParser(new Object[]{
				new ByteArrayOutputStreamPayload(Subscribe.PSUBSCRIBE),
				new ByteArrayOutputStreamPayload(KeeperPubSubConstants.PSUBSCRIBE_PATTERN),
				1L
		}).format());
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

}
