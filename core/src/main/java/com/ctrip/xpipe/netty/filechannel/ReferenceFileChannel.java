package com.ctrip.xpipe.netty.filechannel;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.utils.ControllableFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *
 *         Nov 9, 2016
 */
public class ReferenceFileChannel implements Closeable, Releasable {

	protected Logger logger = LoggerFactory.getLogger(getClass());

	private AtomicLong reference = new AtomicLong();

	private AtomicBoolean closed = new AtomicBoolean(false);
	
	private AtomicLong currentPos = new AtomicLong(0L);

	private ControllableFile file;

	public ReferenceFileChannel(ControllableFile file) throws FileNotFoundException {
		this(file, 0L);
	}

	public ReferenceFileChannel(ControllableFile file, long startPos) throws FileNotFoundException {

		this.file = file;
		this.currentPos.set(startPos);
	}

	@Override
	public void close() throws IOException {
		
		logger.debug("[close]{}", this);
		closed.set(true);
		tryCloseChannel();
	}

	public DefaultReferenceFileRegion read(long maxBytes) throws IOException {

		while(true){
			
			final long fileEnd = file.size();
			final long previousPos = currentPos.get();
			
			long end = fileEnd;
			if(maxBytes > 0){
				end = Math.min(fileEnd, previousPos + maxBytes);
			}
			
			if(currentPos.compareAndSet(previousPos, end)){
				
				increase();
				if(end - previousPos < 0){
					logger.warn("[readTilEnd]pre:{}, end:{}, filelen:{}", previousPos, end, fileEnd);
				}
				return new DefaultReferenceFileRegion(file.getFileChannel(), previousPos, end - previousPos, this);
			}
		}
	}

	public DefaultReferenceFileRegion readTilEnd() throws IOException {
		return read(-1);
	}

	private void increase() {
		reference.incrementAndGet();
	}

	private void tryCloseChannel() {

		if (closed.get() && reference.get() <= 0) {
			try {
				logger.info("[tryCloseChannel][doClose]{}", file);
				file.close();
			} catch (IOException e) {
				logger.error("[tryCloseChannel]" + file, e);
			}
		}
	}

	@Override
	public void release() throws Exception {

		long current = reference.decrementAndGet();

		if (current <= 0) {
			tryCloseChannel();
		}
		
		if(current < 0){
			logger.error("[release][current < 0]{}, {}", file, current);
		}
	}
	

	protected boolean isFileChannelClosed(){
		
		return !file.isOpen();
	}
	
	public boolean hasAnythingToRead() throws IOException{

		long fileSize = file.size();
		long current = currentPos.get();

		if(current > fileSize){
			throw new IllegalStateException("currentPos > fileSize + " + current + ">" + fileSize);
		}
		
		return current < fileSize;
	}

	public long position() {
	    return currentPos.get();
	}
	
	@Override
	public String toString() {
		return String.format("file:%s, pos:%d", file, currentPos.get());
	}
	
}
