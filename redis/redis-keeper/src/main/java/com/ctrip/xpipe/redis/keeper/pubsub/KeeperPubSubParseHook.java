package com.ctrip.xpipe.redis.keeper.pubsub;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.stream.ResyncingCommandParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * 复制流字节 → {@link ResyncingCommandParser} → {@code RedisOpType.PUBLISH} → {@link KeeperPubSubListener}。
 * 解析失败只 WARN + 打点，不抛给写盘 / 复制路径（D17 / D19 / AC-21）。
 */
public class KeeperPubSubParseHook {

	private static final Logger logger = LoggerFactory.getLogger(KeeperPubSubParseHook.class);

	public static final String MONITOR_TYPE = "KeeperPubSubParse";

	public static final String MONITOR_PARSE_FAIL = "parseFail";

	private final RedisOpParser redisOpParser;

	private final KeeperPubSubListener listener;

	private final ResyncingCommandParser commandParser;

	public KeeperPubSubParseHook(RedisOpParser redisOpParser, KeeperPubSubListener listener) {
		this.redisOpParser = Objects.requireNonNull(redisOpParser, "redisOpParser");
		this.listener = Objects.requireNonNull(listener, "listener");
		this.commandParser = new ResyncingCommandParser(this::onParsedCommand);
	}

	public void onCommands(ByteBuf buf) {
		if (buf == null || !buf.isReadable()) {
			return;
		}
		try {
			commandParser.doRead(buf);
		} catch (Throwable th) {
			logger.warn("[onCommands]", th);
			EventMonitor.DEFAULT.logEvent(MONITOR_TYPE, MONITOR_PARSE_FAIL);
		}
	}

	public void reset() {
		commandParser.reset();
	}

	private void onParsedCommand(Object[] payload, ByteBuf commandBuf) {
		try {
			RedisOp redisOp = redisOpParser.parse(payload);
			if (redisOp == null || !RedisOpType.PUBLISH.equals(redisOp.getOpType())) {
				return;
			}
			if (!(redisOp instanceof RedisSingleKeyOp)) {
				return;
			}
			RedisSingleKeyOp publish = (RedisSingleKeyOp) redisOp;
			RedisKey key = publish.getKey();
			byte[] value = publish.getValue();
			if (key == null || key.get() == null || value == null) {
				return;
			}
			listener.onPublish(
					new String(key.get(), StandardCharsets.ISO_8859_1),
					new String(value, StandardCharsets.ISO_8859_1));
		} catch (Throwable th) {
			logger.warn("[onParsedCommand] skip", th);
		}
	}
}
