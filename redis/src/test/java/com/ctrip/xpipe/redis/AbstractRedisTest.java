package com.ctrip.xpipe.redis;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;
import com.ctrip.xpipe.redis.tools.SimpleFileReplicationStore;

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
		rdbFile = tmpDir + "/" + "rdbFile" + UUID.randomUUID().toString() + ".rdb";
		commandFile = tmpDir + "/" + "commandFile" + UUID.randomUUID().toString() + ".command";
		return new SimpleFileReplicationStore(rdbFile, commandFile);
		
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
