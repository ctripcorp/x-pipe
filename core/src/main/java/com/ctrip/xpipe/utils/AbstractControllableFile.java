package com.ctrip.xpipe.utils;


import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author wenchao.meng
 *
 * Jan 5, 2017
 */
public abstract class AbstractControllableFile implements ControllableFile{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private File file;
	
	private AtomicReference<RandomAccessFile> randomAccessFile = new AtomicReference<RandomAccessFile>(null);
	
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
		if(randomAccessFile.get() != null){
			logger.info("[doClose]{}", file);
			randomAccessFile.get().close();
		}
	}

	@Override
	public long size() {

		try {
			return getFileChannel().size();
		} catch (FileNotFoundException e){
			throw new XpipeRuntimeException(String.format("file not found:%s", file), e);
		} catch (IOException e) {
			logger.warn("error get file size, use file.length:" + file, e);
		}

		if(!file.exists()){
			throw new XpipeRuntimeException(String.format("file not found:%s", file));
		}
		return file.length();
	}
	
	protected void tryOpen() throws IOException{
		
		if(randomAccessFile.get() == null){
			doOpen();
		}else if(!randomAccessFile.get().getChannel().isOpen()){
			logger.debug("[tryOpen][file closed, reopen it]{}", file);
			doOpen();
		}
	}

	protected synchronized void doOpen() throws IOException {
		
		if(randomAccessFile.get() != null && randomAccessFile.get().getChannel().isOpen()){
			return;
		}
		
		logger.info("[doOpen]{}", file);
		closed.set(false);
		randomAccessFile.set(new RandomAccessFile(file, "rw"));
		FileChannel fileChannel = randomAccessFile.get().getChannel();
		fileChannel.position(fileChannel.size());
	}

	@Override
	public FileChannel getFileChannel() throws IOException{
		
		tryOpen();
		return randomAccessFile.get().getChannel();
	}
	
	@Override
	public boolean isOpen() {
		return !closed.get();
	}
	
	@Override
	public String toString() {
		return FileUtils.shortPath(file.getPath());
	}

	@Override
	public void setLength(int size) throws IOException {
		tryOpen();
		randomAccessFile.get().setLength(size);
	}
}
