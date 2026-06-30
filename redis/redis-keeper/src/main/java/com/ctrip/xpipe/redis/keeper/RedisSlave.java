package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.server.PartialAware;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationProgress;
import io.netty.channel.ChannelFuture;


/**
 * @author wenchao.meng
 *
 * May 20, 2016 3:55:37 PM
 */
public interface RedisSlave extends RedisClient<RedisKeeperServer>, PartialAware, CommandsListener{
	
	void waitForRdbDumping();

	void waitForSeqFsync();
	
	SLAVE_STATE getSlaveState();

	void ack(Long valueOf, boolean putOnline);
	
	Long getAck();
	
	Long getAckTime();

	void beginWriteCommands(ReplicationProgress<?> progress);

	void beginWriteRdb(EofType eofType, ReplicationProgress<?> rdbProgress);

	ChannelFuture writeFile(ReferenceFileRegion referenceFileRegion);

	void rdbWriteComplete();

	void partialSync();
	
	void processPsyncSequentially(Runnable runnable);

	/**
	 * if partial sync, do real close after continue sent
	 */
	void markPsyncProcessed();

	/**
	 * If psync ? -1 or psync xxxx 1, slave start with no data, we should fsync immediately
	 */
	void markColdStart();

	boolean isColdStart();

	String metaInfo();

	boolean supportProgress(Class<? extends ReplicationProgress<?>> clazz);

	boolean supportRdb(RdbStore.Type rdbType);

}
