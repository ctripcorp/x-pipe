package com.ctrip.xpipe.redis.keeper.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author wenchao.meng
 *
 *         Dec 7, 2016
 */
public class CommandFileContext {
	
	private static Logger logger = LoggerFactory.getLogger(CommandFileContext.class);

	private final long currentStartOffset;

	private RandomAccessFile writeFile;

	private FileChannel channel;

	private File currentFile;

	public CommandFileContext(long currentStartOffset, File currentFile) throws IOException {
		this.currentStartOffset = currentStartOffset;
		this.currentFile = currentFile;
		openFile();
	}
	
	private void openFile() throws IOException {
		
		writeFile = new RandomAccessFile(currentFile, "rw");
		channel = writeFile.getChannel();
		channel.position(channel.size());
	}

	public FileChannel getChannel() throws IOException {
		if(!channel.isOpen()){
			logger.info("[getChannel][channel closed, open channel]{}", currentFile);
			openFile();
		}
		return channel;
	}

	public void close() throws IOException {
		channel.close();
		writeFile.close();
	}
	
	public long fileLength(){
		try {
			return getChannel().size();
		}catch (IOException e) {
			throw new XpipeRuntimeException(String.format("%s", currentFile), e);
		}
	}

	public long totalLength() {
		return currentStartOffset + fileLength();
	}

}
