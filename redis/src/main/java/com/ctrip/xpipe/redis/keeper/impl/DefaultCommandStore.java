package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;

public class DefaultCommandStore implements CommandStore {

	private final static Logger logger = LogManager.getLogger(DefaultCommandStore.class);

	private File baseDir;

	private String fileNamePrefix;

	private long currentStartOffset;

	private int maxFileSize;

	private RandomAccessFile writeFile;

	private FileChannel channel;

	private FilenameFilter fileFilter;

	public DefaultCommandStore(File file, int maxFileSize) throws IOException {
		this.baseDir = file.getParentFile();
		this.fileNamePrefix = file.getName();
		this.maxFileSize = maxFileSize;
		fileFilter = new PrefixFileFilter(fileNamePrefix);

		currentStartOffset = findMaxStartOffset();
		File currentFile = fileForStartOffset(currentStartOffset);
		logger.info("Write to " + currentFile.getName());
		writeFile = new RandomAccessFile(currentFile, "rw");
		channel = writeFile.getChannel();
		// append to file
		channel.position(channel.size());
	}

	private File fileForStartOffset(long startOffset) {
		return new File(baseDir, fileNamePrefix + "_" + startOffset);
	}

	private long findMaxStartOffset() {
		long maxStartOffset = 0;
		File[] files = baseDir.listFiles((FilenameFilter) fileFilter);
		for (File file : files) {
			long startOffset = extractStartOffset(file);
			if (startOffset > maxStartOffset) {
				maxStartOffset = startOffset;
			}
		}
		return maxStartOffset;
	}

	private long extractStartOffset(File file) {
		return Long.parseLong(file.getName().substring(fileNamePrefix.length() + 1));
	}

	@Override
	public int appendCommands(ByteBuf byteBuf) throws IOException {
		rotateFileIfNenessary();

		int wrote = 0;
		ByteBuffer[] buffers = byteBuf.nioBuffers();
		if (buffers != null) {
			for (ByteBuffer buf : buffers) {
				wrote += channel.write(buf);
			}
		}

		return wrote;
	}

	private void rotateFileIfNenessary() throws IOException {
		if (writeFile.length() >= maxFileSize) {
			currentStartOffset += writeFile.length();
			writeFile.close();
			File newFile = new File(baseDir, fileNamePrefix + "_" + currentStartOffset);
			logger.info("Rotate to " + newFile.getName());
			writeFile = new RandomAccessFile(newFile, "rw");
			channel = writeFile.getChannel();
		}
	}

	@Override
	public CommandReader beginRead(long startOffset) throws IOException {
		File targetFile = findFileForOffset(startOffset);
		if (targetFile == null) {
			// TODO
			throw new IllegalArgumentException(startOffset + " is illegal");
		}
		long fileStartOffset = extractStartOffset(targetFile);
		long channelPosition = startOffset - fileStartOffset;
		return new DefaultCommandReader(targetFile, channelPosition);
	}

	private File findFileForOffset(long targetStartOffset) {
		File[] files = baseDir.listFiles((FilenameFilter) fileFilter);
		for (File file : files) {
			long startOffset = extractStartOffset(file);
			if (targetStartOffset >= startOffset
			      && (file.length() == 0 || targetStartOffset < startOffset + file.length())) {
				return file;
			}
		}

		return null;
	}

	private File findNextFile(File curFile) {
		long startOffset = extractStartOffset(curFile);
		File nextFile = fileForStartOffset(startOffset + curFile.length());
		if (nextFile.isFile()) {
			return nextFile;
		} else {
			return null;
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	private class DefaultCommandReader implements CommandReader {

		private RandomAccessFile readFile;

		private FileChannel channel;

		private File curFile;

		public DefaultCommandReader(File curFile, long initChannelPosition) throws IOException {
			this.curFile = curFile;
			readFile = new RandomAccessFile(curFile, "r");
			channel = readFile.getChannel();
			channel.position(initChannelPosition);
		}

		@Override
		public void close() throws IOException {
			readFile.close();
		}

		@Override
		public int read(ByteBuffer dst) throws IOException {
			readNextFileIfNecessary();
			return channel.read(dst);
		}

		private void readNextFileIfNecessary() throws IOException {
			if (channel.size() > 0 && channel.position() == channel.size()) {
				// TODO notify when next file ready
				File nextFile = findNextFile(curFile);
				if (nextFile != null) {
					curFile = nextFile;
					readFile.close();
					readFile = new RandomAccessFile(curFile, "r");
					channel = readFile.getChannel();
				}
			}
		}
	}
}
