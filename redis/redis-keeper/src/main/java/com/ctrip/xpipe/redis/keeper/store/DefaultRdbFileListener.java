package com.ctrip.xpipe.redis.keeper.store;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.ctrip.xpipe.redis.keeper.RdbFileListener;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.protocal.cmd.Psync;
import com.ctrip.xpipe.redis.keeper.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author wenchao.meng
 *
 * 2016年5月9日 下午5:31:00
 */
public class DefaultRdbFileListener implements RdbFileListener{
	
	private static Logger logger = LoggerFactory.getLogger(DefaultRdbFileListener.class);
	
	private RedisSlave redisSlave;
	
	private AtomicLong writtenLength = new AtomicLong();
	
	private AtomicBoolean stop = new AtomicBoolean(false);
	
	public DefaultRdbFileListener(RedisSlave redisSlave) {
		this.redisSlave = redisSlave;
	}

	@Override
	public void onFileData(FileChannel fileChannel, long pos, long len) {
		
		if(len == -1){
			
			if(logger.isInfoEnabled()){
				logger.info("[rdb write complete]" + redisSlave + "," + writtenLength);
			}
			redisSlave.rdbWriteComplete();
			return;
		}
		writtenLength.addAndGet(len);
		redisSlave.writeFile(fileChannel, pos, len);
	}

	
	@Override
	public void setRdbFileInfo(long rdbFileSize, long rdbFileKeeperOffset) {
		
		if(logger.isInfoEnabled()){
			logger.info("[setRdbFileInfo]rdbFileSize:" + rdbFileSize + ",rdbFileOffset:" + rdbFileKeeperOffset);
		}

		SimpleStringParser simpleStringParser = new SimpleStringParser(
				StringUtil.join(" ", Psync.FULL_SYNC, redisSlave.getRedisKeeperServer().getKeeperRunid(), String.valueOf(rdbFileKeeperOffset)));
		
		logger.info("[setRdbFileInfo]{},{}", simpleStringParser.getPayload(), redisSlave);
		redisSlave.sendMessage(simpleStringParser.format());
		
		redisSlave.beginWriteRdb(rdbFileSize, rdbFileKeeperOffset);

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
		
		logger.error("[exception][close client]" + redisSlave, e);
		try {
			redisSlave.close();
		} catch (IOException e1) {
			logger.error("[exception]" + redisSlave, e1);
		}
	}
}
