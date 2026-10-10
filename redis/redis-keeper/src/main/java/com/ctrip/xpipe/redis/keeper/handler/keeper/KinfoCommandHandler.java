/**
 * 
 */
package com.ctrip.xpipe.redis.keeper.handler.keeper;


import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.protocal.protocal.CommandBulkStringParser;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;

/**
 * @author marsqing
 *
 *         Jun 1, 2016 11:08:30 AM
 */
public class KinfoCommandHandler extends AbstractCommandHandler {

	@Override
	public String[] getCommands() {
		return new String[] { "kinfo" };
	}

	@Override
	protected void doHandle(String[] args, RedisClient redisClient) {
		RedisKeeperServer keeper = (RedisKeeperServer) redisClient.getRedisServer();

		// 只读命令不得触发 store 构造：getReplicationStore() 会经由 getCurrent() 做
		// mkdir(baseDir) + 以 READ_WRITE 打开共享目录（TFS 模式下即取得写租约）。
		// 未分配角色（UNKNOWN / PRE_*）或 PREPARE 时没有已打开的 store —— 明确报错，不抛。
		ReplicationStore opened = keeper.getOpenedStore();
		if (opened == null) {
			logger.warn("[doHandle][store not opened]{}", keeper);
			redisClient.sendMessage(new CommandBulkStringParser("ERR store not opened").format());
			return;
		}

		String result = Codec.DEFAULT.encode(opened.getMetaStore().dupReplicationStoreMeta());

		logger.info("[doHandle]{}", result);
		redisClient.sendMessage(new CommandBulkStringParser(result).format());
	}

	@Override
	public boolean support(RedisServer server) {
		return server instanceof RedisKeeperServer;
	}

}
