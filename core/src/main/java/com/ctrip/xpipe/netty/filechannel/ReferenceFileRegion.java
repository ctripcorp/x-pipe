package com.ctrip.xpipe.netty.filechannel;

import io.netty.channel.DefaultFileRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.FileChannel;

/**
 * @author wenchao.meng
 *
 * Nov 10, 2016
 */
public class ReferenceFileRegion extends DefaultFileRegion{
	
	protected static Logger logger = LoggerFactory.getLogger(ReferenceFileRegion.class);
	
	private ReferenceFileChannel referenceFileChannel;
	
	/**
	 * for debug purpose, ignore
	 */
	private long totalPos;
	
	public ReferenceFileRegion(FileChannel fileChannel, long position, long count, ReferenceFileChannel referenceFileChannel) {
		
		super(fileChannel, position, count);
		this.referenceFileChannel = referenceFileChannel;
	}

	
	@Override
	public void deallocate() {
		
		try {
			referenceFileChannel.release();
		} catch (Exception e) {
			logger.error("[deallocate]" + referenceFileChannel, e);
		}
	}
	
	@Override
	public String toString() {
		
		return String.format("(%s), pos:%d, len:%d", referenceFileChannel, position(), count());
	}


	public long getTotalPos() {
		return totalPos;
	}


	public void setTotalPos(long totalPos) {
		this.totalPos = totalPos;
	}

}
