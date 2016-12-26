package com.ctrip.xpipe.redis.keeper.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileChannel;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.RdbFileListener;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.RdbStoreListener;

import io.netty.buffer.ByteBuf;

public class DefaultRdbStore implements RdbStore {

	private final static Logger logger = LoggerFactory.getLogger(DefaultRdbStore.class);

	private RandomAccessFile writeFile;

	protected File file;

	private FileChannel channel;

	protected EofType eofType;

	private AtomicReference<Status> status = new AtomicReference<>(Status.Writing);

	protected long rdbLastKeeperOffset;

	private AtomicInteger refCount = new AtomicInteger(0);
	
	private List<RdbStoreListener> rdbStoreListeners = new LinkedList<>();
	
	public DefaultRdbStore(File file, long rdbLastKeeperOffset, EofType eofType) throws IOException {

		this.file = file;
		this.eofType = eofType;
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
	public void truncate(int reduceLen) throws IOException {
		channel.truncate(channel.size() - reduceLen);
	}

	@Override
	public void endRdb() throws IOException {
		try{
			checkAndSetRdbState();
		}finally{
			for(RdbStoreListener listener : rdbStoreListeners){
				listener.onEndRdb();
			}
			writeFile.close();
		}
	}

	private void checkAndSetRdbState() {
		
		//TODO check file format
		long actualFileLen = file.length();
		
		if(eofType.fileOk(file)){
			status.set(Status.Success);
		} else {
			logger.error("[endRdb]actual:{}, expected:{}, file:{}", actualFileLen, eofType, file);
			status.set(Status.Fail);
			throw new RdbStoreExeption(eofType, file);
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
		
		rdbFileListener.setRdbFileInfo(eofType, rdbLastKeeperOffset);

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

	private boolean isRdbWriting(Status status) {
		return status != Status.Fail && status != Status.Success;
	}

	@Override
	public int refCount() {
		return refCount.get();
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
		return String.format("eofType:%s, rdbLastKeeperOffset:%d,file:%s, status:%s", eofType, rdbLastKeeperOffset, file, status.get());
	}

	@Override
	public void destroy() throws Exception {
		
		logger.info("[destroy][delete file]{}", file);
		file.delete();
	}

	@Override
	public void close() throws IOException {
		
		logger.info("[close]{}", file);
		if(writeFile != null){
			writeFile.close();
		}
	}

	@Override
	public void addListener(RdbStoreListener rdbStoreListener) {
		rdbStoreListeners.add(rdbStoreListener);
	}

	@Override
	public void removeListener(RdbStoreListener rdbStoreListener) {
		rdbStoreListeners.remove(rdbStoreListener);
	}

}
