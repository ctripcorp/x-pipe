package com.ctrip.xpipe.redis.keeper.config;

/**
 * Keeper Pub/Sub 代码常量（m5 D16 / D18）。禁止魔法值散落。
 */
public final class KeeperPubSubConstants {

	public static final int MAX_SUBSCRIBE_CHANNELS = 10;

	public static final int PUBSUB_DELIVER_QUEUE_SIZE = 1024;

	public static final String PSUBSCRIBE_PATTERN = "*";

	private KeeperPubSubConstants() {
	}
}
