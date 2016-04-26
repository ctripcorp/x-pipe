package com.ctrip.xpipe.redis;



import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.impl.DefaultReplicationStore;

import io.netty.buffer.ByteBufAllocator;

/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午5:54:09
 */
public abstract class AbstractRedisTest extends AbstractTest{

	protected ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

	private String rdbFile, commandFile;
	
	protected static final int runidLength = 40;

	protected ReplicationStore createReplicationStore(){
		
		String tmpDir = getTestFileDir();
		
		if(logger.isInfoEnabled()){
			logger.info("[createReplicationStore]" + tmpDir);
		}
		return new DefaultReplicationStore(new File(tmpDir));
		
	}
	
	protected String getRdbFile() {
		return rdbFile;
	}
	
	public String getCommandFile() {
		return commandFile;
	}
	
	protected String readLine(InputStream ins) throws IOException {
		
		StringBuilder sb = new StringBuilder();
		int last = 0;
		while(true){
			
			int data = ins.read();
			if(data == -1){
				return null;
			}
			sb.append((char)data);
			if(data == '\n' && last == '\r'){
				break;
			}
			last = data;
		}
		
		return sb.toString();
	}

	

}
