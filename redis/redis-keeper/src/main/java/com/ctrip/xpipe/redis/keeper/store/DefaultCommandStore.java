package com.ctrip.xpipe.redis.keeper.store;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.store.CommandReader;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.utils.OffsetNotifier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class DefaultCommandStore implements CommandStore {

	private final static Logger logger = LoggerFactory.getLogger(DefaultCommandStore.class);

	private final File baseDir;

	private final String fileNamePrefix;

	private final int maxFileSize;

	private final FilenameFilter fileFilter;

	private final ConcurrentMap<DefaultCommandReader, Boolean> readers = new ConcurrentHashMap<>();

	private final OffsetNotifier offsetNotifier;

	private AtomicReference<CommandFileContext> cmdFileCtxRef = new AtomicReference<>();

	public DefaultCommandStore(File file, int maxFileSize) throws IOException {
		this.baseDir = file.getParentFile();
		this.fileNamePrefix = file.getName();
		this.maxFileSize = maxFileSize;
		fileFilter = new PrefixFileFilter(fileNamePrefix);

		long currentStartOffset = findMaxStartOffset();
		File currentFile = fileForStartOffset(currentStartOffset);
		logger.info("Write to " + currentFile.getName());
		CommandFileContext cmdFileCtx = new CommandFileContext(currentStartOffset, currentFile);
		cmdFileCtxRef.set(cmdFileCtx);
		offsetNotifier = new OffsetNotifier(currentStartOffset + cmdFileCtx.channel.size() - 1);
	}

	private File fileForStartOffset(long startOffset) {
		return new File(baseDir, fileNamePrefix + startOffset);
	}

	private long findMaxStartOffset() {
		long maxStartOffset = 0;
		File[] files = baseDir.listFiles((FilenameFilter) fileFilter);
		if (files != null) {
			for (File file : files) {
				long startOffset = extractStartOffset(file);
				if (startOffset > maxStartOffset) {
					maxStartOffset = startOffset;
				}
			}
		}
		return maxStartOffset;
	}

	public long extractStartOffset(File file) {
		return Long.parseLong(file.getName().substring(fileNamePrefix.length()));
	}

	@Override
	public int appendCommands(ByteBuf byteBuf) throws IOException {
		rotateFileIfNenessary();

		CommandFileContext cmdFileCtx = cmdFileCtxRef.get();
		int wrote = 0;
		ByteBuffer[] buffers = byteBuf.nioBuffers();
		// TODO ensure all read
		if (buffers != null) {
			for (ByteBuffer buf : buffers) {
				wrote += cmdFileCtx.channel.write(buf);
			}
		}

		byteBuf.readerIndex(byteBuf.writerIndex());

		offsetNotifier.offsetIncreased(cmdFileCtx.currentStartOffset + cmdFileCtx.channel.size());

		return wrote;
	}

	@Override
	public long totalLength() {
		return cmdFileCtxRef.get().totalLength();
	}

	private void rotateFileIfNenessary() throws IOException {
		CommandFileContext curCmdFileCtx = cmdFileCtxRef.get();
		if (curCmdFileCtx.writeFile.length() >= maxFileSize) {
			long newStartOffset = curCmdFileCtx.currentStartOffset + curCmdFileCtx.writeFile.length();
			File newFile = new File(baseDir, fileNamePrefix + newStartOffset);
			cmdFileCtxRef.set(new CommandFileContext(newStartOffset, newFile));
			logger.info("Rotate to " + newFile.getName());

			curCmdFileCtx.close();
		}
	}

	@Override
	public CommandReader beginRead(long startOffset) throws IOException {
		File targetFile = findFileForOffset(startOffset);
		if (targetFile == null) {
			throw new IOException("File for offset " + startOffset + " in dir " + baseDir + " does not exist");
		}
		long fileStartOffset = extractStartOffset(targetFile);
		long channelPosition = startOffset - fileStartOffset;
		DefaultCommandReader reader = new DefaultCommandReader(targetFile, channelPosition, offsetNotifier);
		readers.put(reader, Boolean.TRUE);
		return reader;
	}

	private File findFileForOffset(long targetStartOffset) throws IOException {

		rotateFileIfNenessary();

		File[] files = baseDir.listFiles((FilenameFilter) fileFilter);
		if (files != null) {
			for (File file : files) {
				long startOffset = extractStartOffset(file);
				if (targetStartOffset >= startOffset && (targetStartOffset < startOffset + file.length() || targetStartOffset < startOffset + maxFileSize)) {
					return file;
				}
			}
		}
		
		if(files != null){
			for(File file : files){
				logger.info("[findFileForOffset]{}, {}", file.getName(), file.length());
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

	private class CommandFileContext {
		public final long currentStartOffset;

		public final RandomAccessFile writeFile;

		public final FileChannel channel;

		public CommandFileContext(long currentStartOffset, File currentFile) throws IOException {
			this.currentStartOffset = currentStartOffset;
			writeFile = new RandomAccessFile(currentFile, "rw");
			channel = writeFile.getChannel();
			// append to file
			channel.position(channel.size());
		}

		public void close() throws IOException {
			channel.close();
			writeFile.close();
		}

		public long totalLength() {
			try {
				return currentStartOffset + channel.size();
			} catch (IOException e) {
				throw new XpipeRuntimeException("[totalLength]getFileLength error" + channel, e);
			}
		}
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
			readers.remove(this);
			readFile.close();
		}

		@Override
		public int read(ByteBuffer dst) throws IOException {
			readNextFileIfNecessary();
			try {
				offsetNotifier.await(curPosition + 1);
			} catch (InterruptedException e) {
				logger.error("[read]", e);
				Thread.currentThread().interrupt();
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

		public File getCurFile() {
			return curFile;
		}
		
		@Override
		public String toString() {
			return "curFile:" + curFile;
		}

	}

	@Override
	public boolean awaitCommandsOffset(long offset, int timeMilli) throws InterruptedException {
		return offsetNotifier.await(offset, timeMilli);
	}

	@Override
	public long lowestReadingOffset() {
		long lowestReadingOffset = Long.MAX_VALUE;

		for (DefaultCommandReader reader : readers.keySet()) {
			File readingFile = reader.getCurFile();
			if (readingFile != null) {
				lowestReadingOffset = Math.min(lowestReadingOffset, extractStartOffset(readingFile));
			}
		}

		return lowestReadingOffset;
	}

	@Override
	public void addCommandsListener(long offset, CommandsListener listener) throws IOException {
		logger.info("[addCommandsListener] from offset {}, {}", offset, listener);

		CommandReader cmdReader = null;

		try {
			cmdReader = beginRead(offset);
		} finally {
			// ensure beforeCommand() is always called
			listener.beforeCommand();
		}

		logger.info("[addCommandsListener] from offset {}, {}", offset, cmdReader);
		
		try {
			// TODO stop notifier
			while (listener.isOpen()) {
				int read = 0;
				ByteBuffer dst = ByteBuffer.allocate(4096);
				read = cmdReader.read(dst);
				logger.debug("[addCommandsListener] {}", read);
				if (read > 0) {
					dst.flip();
					listener.onCommand(Unpooled.wrappedBuffer(dst));
				} else {
					try {
						// TODO
						Thread.sleep(100);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
			}
		} catch (Throwable th) {
			logger.error("[readCommands][exit]" + listener, th);
		} finally {
			cmdReader.close();
		}
	}

}
