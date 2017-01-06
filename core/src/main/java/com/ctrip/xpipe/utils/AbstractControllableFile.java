package com.ctrip.xpipe.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.utils.ControllableFile;

/**
 * @author wenchao.meng
 *
 * Jan 5, 2017
 */
public abstract class AbstractControllableFile implements ControllableFile{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private File file;
	
	private RandomAccessFile randomAccessFile;
	
	private AtomicBoolean closed = new AtomicBoolean(false);

	public AbstractControllableFile(String file) {
		this(new File(file));
	}

	public AbstractControllableFile(File file) {
		this.file = file;
	}
	
	@Override
	public void close() throws IOException {
		
		closed.set(true);
		if(randomAccessFile != null){
			randomAccessFile.close();
		}
	}

	@Override
	public long size() throws IOException {
		return getFileChannel().size();
	}
	
	protected void tryOpen() throws IOException{
		
		if(randomAccessFile == null){
			doOpen();
		}else if(!randomAccessFile.getChannel().isOpen()){
			logger.debug("[tryOpen][file closed, reopen it]{}", file);
			doOpen();
		}
	}

	protected void doOpen() throws FileNotFoundException {
		
		logger.debug("[doOpen]{}", file);
		closed.set(false);
		randomAccessFile = new RandomAccessFile(file, "rw");
	}

	@Override
	public FileChannel getFileChannel() throws IOException{
		
		tryOpen();
		return randomAccessFile.getChannel();
	}
	
	@Override
	public boolean isOpen() {
		return !closed.get();
	}
}
