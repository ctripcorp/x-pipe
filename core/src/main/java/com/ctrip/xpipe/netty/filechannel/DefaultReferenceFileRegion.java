package com.ctrip.xpipe.netty.filechannel;

import io.netty.channel.DefaultFileRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author wenchao.meng
 *
 * Nov 10, 2016
 */
public class DefaultReferenceFileRegion extends DefaultFileRegion implements ReferenceFileRegion {
	
	protected static Logger logger = LoggerFactory.getLogger(DefaultReferenceFileRegion.class);
	
	private ReferenceFileChannel referenceFileChannel;

	// Atomic flag to ensure deallocate() is only executed once
	private final AtomicBoolean deallocated = new AtomicBoolean(false);
	
	/**
	 * for debug purpose, ignore
	 */
	private long totalPos;
	
	public DefaultReferenceFileRegion(FileChannel fileChannel, long position, long count, ReferenceFileChannel referenceFileChannel) {
		
		super(fileChannel, position, count);
		this.referenceFileChannel = referenceFileChannel;
	}

	
	@Override
	public void deallocate() {
		
		try {
			if(deallocated.compareAndSet(false, true)) {
				referenceFileChannel.release();
			} else {
				logger.error("[deallocate][already deallocated] {}", referenceFileChannel);
			}
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

	public boolean isDeallocated() {
		return deallocated.get();
	}

}
