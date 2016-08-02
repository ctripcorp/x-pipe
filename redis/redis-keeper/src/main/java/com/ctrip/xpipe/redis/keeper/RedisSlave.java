package com.ctrip.xpipe.redis.keeper;

import java.nio.channels.FileChannel;

import com.ctrip.xpipe.api.server.PartialAware;


/**
 * @author wenchao.meng
 *
 * May 20, 2016 3:55:37 PM
 */
public interface RedisSlave extends RedisClient, PartialAware{
	
	void setSlaveState(SLAVE_STATE slaveState);
	
	SLAVE_STATE getSlaveState();

	void ack(Long valueOf);
	
	Long getAck();
	
	Long getAckTime();
	
	void beginWriteCommands(long beginOffset);
	
	void beginWriteRdb(long rdbFileSize, long rdbFileOffset);
	
	void writeFile(FileChannel fileChannel, long pos, long len);

	void rdbWriteComplete();

	void partialSync();
	
	void processPsyncSequentially(Runnable runnable);

}
