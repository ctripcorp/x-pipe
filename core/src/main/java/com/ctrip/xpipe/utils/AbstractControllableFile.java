package com.ctrip.xpipe.utils;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
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

	public AbstractControllableFile(String file) throws IOException {
		this(new File(file));
	}

	public AbstractControllableFile(String file, long pos) throws IOException {
		this(new File(file), pos);
	}

	public AbstractControllableFile(File file) throws IOException {
		this(file, 0);
	}

	public AbstractControllableFile(File file, long pos) throws IOException {
		this.file = file;
		if(pos > 0){
			getFileChannel().position(pos);
		}
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
		
		return size(0);
	}
	
	private long size(int depth) throws IOException {
		
		try{
			return getFileChannel().size();
		}catch(ClosedChannelException e){
			if(depth < 2){
				logger.info("[size][closed, reopen]{}", e);
				return size(depth + 1);
			}
			throw e;
		}
	}

	protected void tryOpen() throws IOException{
		
		if(randomAccessFile == null){
			doOpen();
		}else if(!randomAccessFile.getChannel().isOpen()){
			logger.debug("[tryOpen][file closed, reopen it]{}", file);
			doOpen();
		}
	}

	protected void doOpen() throws IOException {
		
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
	
	@Override
	public String toString() {
		return FileUtils.shortPath(file.getPath());
	}
}
