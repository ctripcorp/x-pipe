package com.ctrip.xpipe.redis.keeper.store;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.redis.keeper.RdbFileListener;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.protocal.cmd.Psync;
import com.ctrip.xpipe.redis.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author wenchao.meng
 *
 * 2016年5月9日 下午5:31:00
 */
public class DefaultRdbFileListener implements RdbFileListener{
	
	private static Logger logger = LogManager.getLogger(DefaultRdbFileListener.class);
	
	private RedisClient redisClient;
	
	private AtomicLong writtenLength = new AtomicLong();
	
	private AtomicBoolean stop = new AtomicBoolean(false);
	
	public DefaultRdbFileListener(RedisClient redisClient) {
		this.redisClient = redisClient;
	}

	@Override
	public void onFileData(FileChannel fileChannel, long pos, long len) {
		
		if(len == -1){
			
			if(logger.isInfoEnabled()){
				logger.info("[rdb write complete]" + redisClient + "," + writtenLength);
			}
			redisClient.rdbWriteComplete();
			return;
		}
		writtenLength.addAndGet(len);
		redisClient.writeFile(fileChannel, pos, len);
	}

	
	@Override
	public void setRdbFileInfo(long rdbFileSize, long rdbFileOffset) {
		
		if(logger.isInfoEnabled()){
			logger.info("[setRdbFileInfo]rdbFileSize:" + rdbFileSize + ",rdbFileOffset:" + rdbFileOffset);
		}

		SimpleStringParser simpleStringParser = new SimpleStringParser(
				StringUtil.join(" ", Psync.FULL_SYNC, redisClient.getRedisKeeperServer().getKeeperRunid(), String.valueOf(rdbFileOffset)));
		redisClient.sendMessage(simpleStringParser.format());
		
		redisClient.beginWriteRdb(rdbFileSize, rdbFileOffset);

	}
	
	@Override
   public boolean isStop() {
	   return stop.get();
   }
	
	public void stop() {
		stop.set(true);
	}

	@Override
	public void exception(Exception e) {
		
		logger.error("[exception][close client]" + redisClient, e);
		try {
			redisClient.close();
		} catch (IOException e1) {
			logger.error("[exception]" + redisClient, e1);
		}
	}
}
