package com.ctrip.xpipe.netty.filechannel;

import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.DefaultFileRegion;

/**
 * @author wenchao.meng
 *
 * Nov 10, 2016
 */
public class ReferenceFileRegion extends DefaultFileRegion{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private ReferenceFileChannel referenceFileChannel;
	
	public ReferenceFileRegion(FileChannel fileChannel, long position, long count, ReferenceFileChannel referenceFileChannel) {
		
		super(fileChannel, position, count);
		this.referenceFileChannel = referenceFileChannel;
	}

	
	@Override
	protected void deallocate() {
		
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

}
