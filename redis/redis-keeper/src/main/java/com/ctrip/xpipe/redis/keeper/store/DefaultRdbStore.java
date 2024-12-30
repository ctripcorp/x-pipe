package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.api.utils.FileSize;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileChannel;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofMarkType;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.store.RdbFileListener;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.RdbStoreListener;
import com.ctrip.xpipe.redis.core.store.OffsetReplicationProgress;
import com.ctrip.xpipe.redis.core.store.ratelimit.ReplDelayConfig;
import com.ctrip.xpipe.redis.core.store.ratelimit.SyncRateLimiter;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import com.ctrip.xpipe.utils.SizeControllableFile;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

public class DefaultRdbStore extends AbstractStore implements RdbStore {

	private final static Logger logger = LoggerFactory.getLogger(DefaultRdbStore.class);
	
	public static final long FAIL_RDB_LENGTH = -1; 
	
	private RandomAccessFile writeFile;

	protected File file;

	protected FileChannel channel;

	protected EofType eofType;

	private AtomicReference<Status> status = new AtomicReference<>(Status.Writing);

	protected String replId;

	protected long rdbOffset;

	private AtomicInteger refCount = new AtomicInteger(0);
	
	protected List<RdbStoreListener> rdbStoreListeners = new LinkedList<>();
	
	private Object truncateLock = new Object();

	private AtomicReference<Type> typeRef;

	private AtomicReference<SyncRateLimiter> rateLimiterRef = new AtomicReference<>();
	
	public DefaultRdbStore(File file, String replId, long rdbOffset, EofType eofType) throws IOException {

		this.replId = replId;
		this.file = file;
		this.eofType = eofType;
		this.rdbOffset = rdbOffset;
		this.typeRef = new AtomicReference<>(Type.UNKNOWN);

		if(file.length() > 0){
			checkAndSetRdbState();
		}else{
			writeFile = new RandomAccessFile(file, "rw");
			channel = writeFile.getChannel();
		}
	}

	@Override
	public String getReplId() {
		return replId;
	}

	@Override
	public long getRdbOffset() {
		return rdbOffset;
	}

	@Override
	public EofType getEofType() {
		return this.eofType;
	}

	@Override
	public File getRdbFile() {
		return file;
	}

	@Override
	public void updateRdbType(Type type) {
		this.typeRef.compareAndSet(Type.UNKNOWN, type);
	}

	@Override
	public Type getRdbType() {
		return typeRef.get();
	}

	@Override
	public void attachRateLimiter(SyncRateLimiter rateLimiter) {
		this.rateLimiterRef.set(rateLimiter);
	}

	@Override
	public int writeRdb(ByteBuf byteBuf) throws IOException {
		makeSureOpen();

		SyncRateLimiter rateLimiter = rateLimiterRef.get();
		if (null != rateLimiter) rateLimiter.acquire(byteBuf.readableBytes());

		return ByteBufUtils.writeByteBufToFileChannel(byteBuf, channel);
	}

	@Override
	public void truncateEndRdb(int reduceLen) throws IOException {
		
		getLogger().info("[truncateEndRdb]{}, {}", this, reduceLen);
		
		synchronized (truncateLock) {
			channel.truncate(channel.size() - reduceLen);
			endRdb();
		}
	}

	@Override
	public void endRdb() {
		
		if(status.get() != Status.Writing){
			getLogger().info("[endRdb][already ended]{}, {}, {}", this, file, status);
			return;
		}
		
		try{
			checkAndSetRdbState();
		}finally{
			notifyListenersEndRdb();
			try {
				writeFile.close();
			} catch (IOException e) {
				getLogger().error("[endRdb]" + this, e);
			}
		}
	}

	protected void notifyListenersEndRdb() {
		
		for(RdbStoreListener listener : rdbStoreListeners){
			try{
				listener.onEndRdb();
			}catch(Throwable th){
				getLogger().error("[notifyListenersEndRdb]" + this, th);
			}
		}
	}

	@Override
	public void failRdb(Throwable throwable) {
		
		getLogger().info("[failRdb]" + this, throwable);
		
		if(status.get() != Status.Writing){
			throw new IllegalStateException("already finished with final state:" + status.get());
		}
		
		status.set(Status.Fail);
		notifyListenersEndRdb();
		try {
			writeFile.close();
		} catch (IOException e1) {
			getLogger().error("[failRdb]" + this, e1);
		}
	}

	@Override
	public long rdbFileLength() {
		
		if(status.get() == Status.Fail){
			return FAIL_RDB_LENGTH;
		}
		return file.length();
	}

	private void checkAndSetRdbState() {
		
		//TODO check file format
		if(eofType.fileOk(file)){
			status.set(Status.Success);
			getLogger().info("[checkAndSetRdbState]{}, {}", this, status);
		} else {
			status.set(Status.Fail);
			long actualFileLen = file.length();
			getLogger().error("[checkAndSetRdbState]actual:{}, expected:{}, file:{}, status:{}", actualFileLen, eofType, file, status);
		}
	}

	@Override
	public boolean updateRdbGtidSet(String gtidSet) {
		//just ignore
	    return true;
	}

	@Override
	public String getGtidSet() {
		return null;
	}

	@Override
	public boolean isGtidSetInit() {
		return false;
	}

	@Override
	public boolean supportGtidSet() {
		return false;
	}

	@Override
	public void readRdbFile(final RdbFileListener rdbFileListener) throws IOException {
		
		makeSureOpen();

		rdbFileListener.beforeFileData();
		refCount.incrementAndGet();

		try (ReferenceFileChannel channel = new ReferenceFileChannel(createControllableFile())) {
			doReadRdbFileInfo(rdbFileListener);
			doReadRdbFile(rdbFileListener, channel);
		} catch (Exception e) {
			getLogger().error("[readRdbFile]Error read rdb file" + file, e);
			rdbFileListener.exception(e);
		}finally{
			refCount.decrementAndGet();
		}
	}

	protected void doReadRdbFileInfo(RdbFileListener rdbFileListener) {
		if (!rdbFileListener.supportProgress(OffsetReplicationProgress.class)) {
			throw new UnsupportedOperationException("offset progress not support");
		}
		rdbFileListener.setRdbFileInfo(eofType, new OffsetReplicationProgress(rdbOffset));
	}

	protected void doReadRdbFile(RdbFileListener rdbFileListener, ReferenceFileChannel referenceFileChannel) throws IOException {

		long lastLogTime = System.currentTimeMillis();
		RateLimiter rateLimiter = RateLimiter.create(Double.MAX_VALUE);
		while (rdbFileListener.isOpen() && (isRdbWriting(status.get()) || (status.get() == Status.Success && referenceFileChannel.hasAnythingToRead()))) {
			int limitBytes = rdbFileListener.getLimitBytesPerSecond();
			ReferenceFileRegion referenceFileRegion = referenceFileChannel.read(limitBytes);

			if (limitBytes > 0 && referenceFileRegion.count() > 0) {
				if (((int)rateLimiter.getRate()) != limitBytes) rateLimiter.setRate(limitBytes);
				int readBytes = (int)Math.min(referenceFileRegion.count(), limitBytes);
				rateLimiter.acquire(readBytes);
			}
			rdbFileListener.onFileData(referenceFileRegion);
			if(referenceFileRegion.count() <= 0) {
				try {
					Thread.sleep(1);
					long currentTime = System.currentTimeMillis();
					if (currentTime - lastLogTime > 10000) {
						getLogger().info("[doReadRdbFile]status:{}, referenceFileChannel:{}, count:{}, rdbFileListener:{}",
								status.get(), referenceFileChannel, referenceFileRegion.count(), rdbFileListener);
						lastLogTime = currentTime;
					}
				} catch (InterruptedException e) {
					getLogger().error("[doReadRdbFile]" + rdbFileListener, e);
					Thread.currentThread().interrupt();
				}
			}
		}

		getLogger().info("[doReadRdbFile] done with status {}", status.get());

		switch (status.get()) {
			case Success:
				if(file.exists()){//this is necessery because file may be deleted
					rdbFileListener.onFileData(null);
				}else{
					rdbFileListener.exception((new Exception("rdb file not exists now " + file)));
				}
				break;
	
			case Fail:
				rdbFileListener.exception(new Exception("[rdb error]" + file));
				break;
			default:
				rdbFileListener.exception(new Exception("[status not right]" + file + "," + status));
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
	public long rdbOffset() {
		return rdbOffset;
	}
	
	public void incrementRefCount() {
		refCount.incrementAndGet();
	}
	
	public void decrementRefCount() {
		refCount.decrementAndGet();
	}

	@Override
	public boolean checkOk() {
		return status.get() == Status.Writing 
				|| ( status.get() == Status.Success && file.exists());
	}

	@Override
	public void destroy() throws Exception {
		
		getLogger().info("[destroy][delete file]{}", file);
		file.delete();
	}

	@Override
	public void close() throws IOException {
		
		if(cmpAndSetClosed()){
			getLogger().info("[close]{}", file);
			if(writeFile != null){
				writeFile.close();
			}
		}else{
			getLogger().warn("[close][already closed]{}", this);
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

	private ControllableFile createControllableFile() throws IOException {
		
		if(eofType instanceof LenEofType){
			return new DefaultControllableFile(file);
		}else if(eofType instanceof EofMarkType){
			
			return new SizeControllableFile(file, new FileSize() {
				
				@Override
				public long getSize(LongSupplier realSizeProvider) {
					
					long realSize = 0;
					synchronized (truncateLock) {//truncate may make size wrong
						realSize = realSizeProvider.getAsLong();
					}
					
					if(status.get() == Status.Writing){
						
						long ret = realSize - ((EofMarkType)eofType).getTag().length(); 
						getLogger().debug("[getSize][writing]{}, {}", DefaultRdbStore.this, ret);
						return ret < 0 ? 0 : ret;
					}
					return realSize;
				}
			});
		}else{
			throw new IllegalStateException("unknown eoftype:" + eofType.getClass() + "," + eofType);
		}
	}

	@Override
	public String toString() {
		return String.format("type:%s, eofType:%s, rdbOffset:%d,file:%s, exists:%b, status:%s",
				typeRef.get().name(), eofType, rdbOffset, file, file.exists(), status.get());
	}

	@Override
	public boolean sameRdbFile(File file) {
		return this.file.equals(file);
	}

	@Override
	public String getRdbFileName() {
		if (null != file) {
			return file.getName();
		}

		return null;
	}

	@Override
	public boolean isWriting() {
		return isRdbWriting(status.get());
	}

	@Override
	public long getRdbFileLastModified() {
		return file.lastModified();
	}
	
	protected Logger getLogger() {
		return logger;
	}

}