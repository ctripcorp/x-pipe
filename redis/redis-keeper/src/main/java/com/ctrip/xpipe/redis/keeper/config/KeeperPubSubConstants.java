package com.ctrip.xpipe.redis.keeper.config;

/**
 * Keeper Pub/Sub 代码常量（m5 D16 / D18）。禁止魔法值散落。
 */
public final class KeeperPubSubConstants {

	public static final int MAX_SUBSCRIBE_CHANNELS = 10;

	public static final int PUBSUB_DELIVER_QUEUE_SIZE = 1024;

	public static final String PSUBSCRIBE_PATTERN = "*";

	/**
	 * PREPARE {@code PrepareCmdParser} 在 {@code getCurrent()} 为空或 listener 退出后的重试间隔。
	 */
	public static final int PREPARE_CMD_PARSER_RETRY_MILLI = 100;

	private KeeperPubSubConstants() {
	}
}
