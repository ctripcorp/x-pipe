package com.ctrip.xpipe.redis.keeper.store;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.util.OffsetNotifier;

import io.netty.buffer.ByteBuf;

public class DefaultCommandStore implements CommandStore {

	private final static Logger logger = LoggerFactory.getLogger(DefaultCommandStore.class);

	private File baseDir;

	private String fileNamePrefix;

	private long currentStartOffset;

	private int maxFileSize;

	private RandomAccessFile writeFile;

	private FileChannel channel;

	private FilenameFilter fileFilter;

	private OffsetNotifier offsetNotifier;
	
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
		offsetNotifier = new OffsetNotifier(currentStartOffset + channel.size() - 1);
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
		// TODO ensure all read
		if (buffers != null) {
			for (ByteBuffer buf : buffers) {
				wrote += channel.write(buf);
			}
		}
		
		byteBuf.readerIndex(byteBuf.writerIndex());

		offsetNotifier.offsetIncreased(currentStartOffset + channel.size());

		return wrote;
	}

	@Override
	public long totalLength() {
		
		try {
			return currentStartOffset + channel.size();
		} catch (IOException e) {
			throw new XpipeRuntimeException("[totalLength]getFileLength error" + channel, e);
		}
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
		return new DefaultCommandReader(targetFile, channelPosition, offsetNotifier);
	}

	private File findFileForOffset(long targetStartOffset) throws IOException {
		
		rotateFileIfNenessary();
		
		File[] files = baseDir.listFiles((FilenameFilter) fileFilter);
		for (File file : files) {
			long startOffset = extractStartOffset(file);
			if (targetStartOffset >= startOffset
			      && (targetStartOffset < startOffset + file.length() || targetStartOffset < startOffset + maxFileSize)) {
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

		private long curPosition;

		public DefaultCommandReader(File curFile, long initChannelPosition, OffsetNotifier notifier) throws IOException {
			this.curFile = curFile;
			readFile = new RandomAccessFile(curFile, "r");
			channel = readFile.getChannel();
			channel.position(initChannelPosition);
			curPosition = extractStartOffset(curFile) + initChannelPosition;
		}

		@Override
		public void close() throws IOException {
			readFile.close();
		}

		@Override
		public int read(ByteBuffer dst) throws IOException {
			readNextFileIfNecessary();
			try {
				offsetNotifier.await(curPosition + 1);
			} catch (InterruptedException e) {
				// TODO
				return 0;
			}
			int read = channel.read(dst);
			if (read > 0) {
				curPosition += read;
			}
			return read;
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

	@Override
	public boolean await(long offset, int timeMilli) throws InterruptedException {
		return offsetNotifier.await(offset, timeMilli);
	}

	@Override
	public void await(long offset) throws InterruptedException {
		offsetNotifier.await(offset);
	}

}
