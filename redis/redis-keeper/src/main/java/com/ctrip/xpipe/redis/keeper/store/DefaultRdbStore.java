package com.ctrip.xpipe.redis.keeper.store;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.redis.core.store.RdbFileListener;
import com.ctrip.xpipe.redis.core.store.RdbStore;

import io.netty.buffer.ByteBuf;

public class DefaultRdbStore implements RdbStore {

	private final static Logger logger = LoggerFactory.getLogger(DefaultRdbStore.class);

	private RandomAccessFile writeFile;

	private File file;

	private FileChannel channel;

	private long rdbFileSize;

	private AtomicReference<Status> status = new AtomicReference<>(Status.Writing);

	private long rdbLastKeeperOffset;

	private AtomicInteger refCount = new AtomicInteger(0);

	public DefaultRdbStore(File file, long rdbLastKeeperOffset, long rdbFileSize) throws IOException {
		this.file = file;
		this.rdbFileSize = rdbFileSize;
		this.rdbLastKeeperOffset = rdbLastKeeperOffset;
		writeFile = new RandomAccessFile(file, "rw");
		channel = writeFile.getChannel();
	}

	public DefaultRdbStore(File file, long rdbLastKeeperOffset, long rdbFileSize, boolean alreadyWrote) throws IOException {
		this(file, rdbLastKeeperOffset, rdbFileSize);
		endRdb();
	}

	@Override
	public int writeRdb(ByteBuf byteBuf) throws IOException {
		// TODO ByteBuf to ByteBuffer correct?
		int wrote = 0;
		ByteBuffer[] bufs = byteBuf.nioBuffers();
		if (bufs != null) {
			for (ByteBuffer buf : bufs) {
				wrote += channel.write(buf);
			}
		}

		return wrote;
	}

	@Override
	public void endRdb() throws IOException {
		long actualFileLen = writeFile.length();
		writeFile.close();

		if (actualFileLen == rdbFileSize) {
			status.set(Status.Success);
		} else {
			logger.error("[endRdb]actual:{}, expected:{}", actualFileLen, rdbFileSize);
			status.set(Status.Fail);
			throw new RdbStoreExeption(rdbFileSize, actualFileLen);
		}
	}

	@Override
	public void readRdbFile(final RdbFileListener rdbFileListener) throws IOException {
		rdbFileListener.beforeFileData();
		refCount.incrementAndGet();

		try (FileChannel channel = new RandomAccessFile(file, "r").getChannel()) {
			doReadRdbFile(rdbFileListener, channel);
		} catch (Exception e) {
			logger.error("Error read rdb file", e);
		}
	}

	private void doReadRdbFile(RdbFileListener rdbFileListener, FileChannel channel) throws IOException {
		rdbFileListener.setRdbFileInfo(rdbFileSize, rdbLastKeeperOffset);

		long start = 0;
		long lastLogTime = System.currentTimeMillis();
		while (rdbFileListener.isOpen() && (isRdbWriting(status.get()) || start < channel.size())) {
			
			if (channel.size() > start) {
				long end = channel.size();
				rdbFileListener.onFileData(channel, start, end - start);
				start = end;
			} else {
				try {
					Thread.sleep(100);
					long currentTime = System.currentTimeMillis();
					if(currentTime - lastLogTime > 10000){
						logger.info("[doReadRdbFile]status:{}, start:{}, channeSize:{}", status.get(), start, channel.size());
						lastLogTime = currentTime;
					}
				} catch (InterruptedException e) {
				}
			}
		}

		logger.info("[doReadRdbFile] done with status {}", status.get());
		refCount.decrementAndGet();

		switch (status.get()) {
		case Success:
			rdbFileListener.onFileData(channel, start, -1L);
			break;

		case Fail:
			// TODO
			rdbFileListener.exception(new Exception(""));
			break;

		default:
			break;
		}
	}

	/**
	 * @param status
	 * @return
	 */
	private boolean isRdbWriting(Status status) {
		return status != Status.Fail && status != Status.Success;
	}

	@Override
	public int refCount() {
		return refCount.get();
	}

	@Override
	public boolean delete() {
		logger.info("Delete rdb file {}", file);
		return file.delete();
	}

	public File getFile() {
		return file;
	}

	@Override
	public long lastKeeperOffset() {
		return rdbLastKeeperOffset;
	}
	
	public void incrementRefCount() {
		refCount.incrementAndGet();
	}
	
	public void decrementRefCount() {
		refCount.decrementAndGet();
	}

}
