package com.ctrip.xpipe.redis.keeper.pubsub;

/**
 * 命令流解析出 {@code PUBLISH} 后的统一出口（D17 / §4.4.2）。
 * ACTIVE/BACKUP 钩子与 PREPARE {@code PrepareCmdParser} 共用。
 */
public interface KeeperPubSubListener {

	void onPublish(String channel, String message);
}
