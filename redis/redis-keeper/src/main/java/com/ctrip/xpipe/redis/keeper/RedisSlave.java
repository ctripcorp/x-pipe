package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.server.PartialAware;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import io.netty.channel.ChannelFuture;


/**
 * @author wenchao.meng
 *
 * May 20, 2016 3:55:37 PM
 */
public interface RedisSlave extends RedisClient, PartialAware, CommandsListener{
	
	void waitForRdbDumping();
	
	SLAVE_STATE getSlaveState();

	void ack(Long valueOf);
	
	Long getAck();
	
	Long getAckTime();
	
	void beginWriteCommands(long beginOffset);

	void beginWriteCommands(GtidSet excludedGtidSet);
	
	void beginWriteRdb(EofType eofType, long rdbFileOffset);
	
	ChannelFuture writeFile(ReferenceFileRegion referenceFileRegion);

	void rdbWriteComplete();

	void partialSync();
	
	void processPsyncSequentially(Runnable runnable);

	/**
	 * if partial sync, do real close after continue sent
	 */
	void markPsyncProcessed();

	String metaInfo();

}
