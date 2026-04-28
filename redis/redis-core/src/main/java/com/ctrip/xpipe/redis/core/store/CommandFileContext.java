package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.utils.DefaultControllableFile;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *
 *         Dec 7, 2016
 */
public class CommandFileContext {
	
	private final long currentStartOffset;

	private ControllableFile controllableFile;

	private File currentFile;

	private final AtomicLong fileLength = new AtomicLong();


	public CommandFileContext(CommandFile commandFile) throws IOException {
		this(commandFile.getStartOffset(), commandFile.getFile());
	}
	
	public CommandFileContext(long currentStartOffset, File currentFile) throws IOException {
		this.currentFile = currentFile;
		this.currentStartOffset = currentStartOffset;
		this.controllableFile = new DefaultControllableFile(currentFile, currentFile.length());
		this.fileLength.set(controllableFile.size());
	}
	

	public void close() throws IOException {
		controllableFile.close();
	}
	
	//if file not exist, create it
	public void createIfNotExist() throws IOException{
		controllableFile.getFileChannel();
	}

	public void advanceFileLength(long delta) {
		fileLength.addAndGet(delta);
	}
	
	public long fileLength(){
		return fileLength.get();
	}

	public long totalLength() {
		return currentStartOffset + fileLength();
	}

	public FileChannel getChannel() throws IOException {
		return controllableFile.getFileChannel();
	}

	public long getLastModified() {
		return currentFile.lastModified();
	}

	public CommandFile getCommandFile() {
		return new CommandFile(currentFile, currentStartOffset);
	}
	
	@Override
	public String toString() {
		return String.format("%s", currentFile);
	}

}
