package com.ctrip.xpipe.redis.keeper.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileChannel;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.store.RdbFileListener;
import com.ctrip.xpipe.redis.core.store.RdbStore;

import io.netty.buffer.ByteBuf;

public class DefaultRdbStore implements RdbStore {

	private final static Logger logger = LoggerFactory.getLogger(DefaultRdbStore.class);

	private RandomAccessFile writeFile;

	protected File file;

	private FileChannel channel;

	protected long rdbFileSize;

	private AtomicReference<Status> status = new AtomicReference<>(Status.Writing);

	protected long rdbLastKeeperOffset;

	private AtomicInteger refCount = new AtomicInteger(0);
	
	public DefaultRdbStore(File file, long rdbLastKeeperOffset, long rdbFileSize) throws IOException {

		this.file = file;
		this.rdbFileSize = rdbFileSize;
		this.rdbLastKeeperOffset = rdbLastKeeperOffset;
		
		if(file.length() > 0){
			checkAndSetRdbState();
		}else{
			writeFile = new RandomAccessFile(file, "rw");
			channel = writeFile.getChannel();
		}
	}
	
	@Override
	public int writeRdb(ByteBuf byteBuf) throws IOException {
		
		int wrote = ByteBufUtils.writeByteBufToFileChannel(byteBuf, channel);
		
		return wrote;
	}

	@Override
	public void endRdb() throws IOException {
		try{
			checkAndSetRdbState();
		}finally{
			writeFile.close();
		}
	}

	private void checkAndSetRdbState() {
		
		long actualFileLen = file.length();
		if (actualFileLen == rdbFileSize) {
			status.set(Status.Success);
		} else {
			logger.error("[endRdb]actual:{}, expected:{}, file:{}", actualFileLen, rdbFileSize, file);
			status.set(Status.Fail);
			throw new RdbStoreExeption(rdbFileSize, actualFileLen);
		}
		
	}

	@Override
	public void readRdbFile(final RdbFileListener rdbFileListener) throws IOException {
		
		rdbFileListener.beforeFileData();
		refCount.incrementAndGet();

		try (ReferenceFileChannel channel = new ReferenceFileChannel(file)) {
			doReadRdbFile(rdbFileListener, channel);
		} catch (Exception e) {
			logger.error("[readRdbFile]Error read rdb file" + file, e);
		}finally{
			refCount.decrementAndGet();
		}
	}

	private void doReadRdbFile(RdbFileListener rdbFileListener, ReferenceFileChannel referenceFileChannel) throws IOException {
		
		rdbFileListener.setRdbFileInfo(rdbFileSize, rdbLastKeeperOffset);

		long lastLogTime = System.currentTimeMillis();
		while (rdbFileListener.isOpen() && (isRdbWriting(status.get()) || (status.get() == Status.Success && referenceFileChannel.hasAnythingToRead()))) {
			
		ReferenceFileRegion referenceFileRegion = referenceFileChannel.readTilEnd();
		
		rdbFileListener.onFileData(referenceFileRegion);
		if(referenceFileRegion.count() <= 0)
			try {
				Thread.sleep(1);
				long currentTime = System.currentTimeMillis();
				if(currentTime - lastLogTime > 10000){
					logger.info("[doReadRdbFile]status:{}, referenceFileChannel:{}, rdbFileListener:{}", status.get(), referenceFileChannel, rdbFileListener);
					lastLogTime = currentTime;
				}
			} catch (InterruptedException e) {
				logger.error("[doReadRdbFile]" + rdbFileListener, e);
				Thread.currentThread().interrupt();
			}
		}

		logger.info("[doReadRdbFile] done with status {}", status.get());

		switch (status.get()) {
		
			case Success:
				rdbFileListener.onFileData(null);
				break;
	
			case Fail:
				rdbFileListener.exception(new Exception("[rdb error]" + file));
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

	@Override
	public File getRdbFile() {
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

	@Override
	public boolean checkOk() {
		return status.get() == Status.Writing || status.get() == Status.Success;
	}

	@Override
	public String toString() {
		return String.format("rdbFileSize:%d, rdbLastKeeperOffset:%d,file:%s, status:%s", rdbFileSize, rdbLastKeeperOffset, file, status.get());
	}

}
