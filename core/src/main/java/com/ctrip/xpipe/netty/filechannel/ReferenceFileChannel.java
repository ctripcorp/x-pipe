package com.ctrip.xpipe.netty.filechannel;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.lifecycle.Releasable;

/**
 * @author wenchao.meng
 *
 *         Nov 9, 2016
 */
public class ReferenceFileChannel implements Closeable, Releasable {

	protected Logger logger = LoggerFactory.getLogger(getClass());

	private AtomicLong reference = new AtomicLong();

	private FileChannel fileChannel;

	private AtomicBoolean closed = new AtomicBoolean(false);
	
	private AtomicLong currentPos = new AtomicLong(0L);

	private File file;

	public ReferenceFileChannel(File file) throws FileNotFoundException {
		this(file, 0L);
	}

	@SuppressWarnings("resource")
	public ReferenceFileChannel(File file, long startPos) throws FileNotFoundException {

		this.file = file;
		RandomAccessFile readFile = new RandomAccessFile(file, "r");
		this.fileChannel = readFile.getChannel();
		this.currentPos.set(startPos);
	}

	@Override
	public void close() throws IOException {
		
		logger.debug("[close]{}", this);
		closed.set(true);
		tryCloseChannel();
	}

	protected ReferenceFileRegion readTilEnd(int maxBytes) throws IOException {

		while(true){
			
			final long fileEnd = fileChannel.size();
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
				return new ReferenceFileRegion(fileChannel, previousPos, end - previousPos, this);
			}
		}
	}

	public ReferenceFileRegion readTilEnd() throws IOException {
		return readTilEnd(-1);
	}

	private void increase() {
		reference.incrementAndGet();
	}

	private void tryCloseChannel() {

		if (closed.get() && reference.get() <= 0) {
			try {
				logger.debug("[tryCloseChannel][doClose]{}", file);
				fileChannel.close();
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
		
		return !fileChannel.isOpen();
	}
	
	public boolean hasAnythingToRead() throws IOException{
		
		return currentPos.get() < fileChannel.size(); 
	}
	
	@Override
	public String toString() {
		return String.format("file:%s, pos:%d", file, currentPos.get());
	}
	
}
