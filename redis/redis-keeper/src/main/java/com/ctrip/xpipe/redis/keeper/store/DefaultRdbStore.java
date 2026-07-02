package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofMarkType;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.core.store.ratelimit.SyncRateLimiter;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;

public class DefaultRdbStore extends AbstractStore implements RdbStore {

	private final static Logger logger = LoggerFactory.getLogger(DefaultRdbStore.class);
	
	public static final long FAIL_RDB_LENGTH = -1; 

	protected File file;

	// write handle for dump (append, atomicReplace=false, lenient=true)
	protected volatile AsyncFile writeAsyncFile;

	// cached read handle for recovered rdb metadata (size/mtime); lazily opened after dump ends
	protected volatile AsyncFile readAsyncFile;

	protected EofType eofType;

	private AtomicReference<Status> status = new AtomicReference<>(Status.Writing);

	protected String replId;

	protected long rdbOffset;

	protected long contiguousBacklogOffset = 0;

	private AtomicInteger refCount = new AtomicInteger(0);
	
	protected List<RdbStoreListener> rdbStoreListeners = new LinkedList<>();
	
	private Object truncateLock = new Object();

	private final Object handleLock = new Object();

	private AtomicReference<Type> typeRef;

	private AtomicReference<SyncRateLimiter> rateLimiterRef = new AtomicReference<>();

	protected final AsyncFileSystem asyncFileSystem;

	protected final IntSupplier asyncWriteMaxBytes;

	public DefaultRdbStore(File file, String replId, long rdbOffset, EofType eofType,
						   AsyncFileSystem asyncFileSystem, IntSupplier asyncWriteMaxBytes) throws IOException {

		this.replId = replId;
		this.file = file;
		this.eofType = eofType;
		this.rdbOffset = rdbOffset;
		this.typeRef = new AtomicReference<>(Type.UNKNOWN);
		this.asyncFileSystem = Objects.requireNonNull(asyncFileSystem, "asyncFileSystem");
		this.asyncWriteMaxBytes = Objects.requireNonNull(asyncWriteMaxBytes, "asyncWriteMaxBytes");

		if (!AsyncFileSystemHelper.await(asyncFileSystem.exists(path()), "exists rdb " + file)) {
			this.writeAsyncFile = openWriteHandle();
		} else {
			this.readAsyncFile = openReadHandle();
			long len = AsyncFileSystemHelper.await(asyncFileSystem.size(readAsyncFile), "size rdb " + file);
			if (len > 0) {
				checkAndSetRdbState();
			} else {
				closeReadHandleQuietly();
				this.writeAsyncFile = openWriteHandle();
			}
		}
	}

	@Override
	public long getContiguousBacklogOffset() {
		return contiguousBacklogOffset;
	}

	@Override
	public void setContiguousBacklogOffset(long contiguousBacklogOffset) {
		this.contiguousBacklogOffset = contiguousBacklogOffset;
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
	public String getGtidLost() {
		return null;
	}

	@Override
	public String getMasterUuid() {
		return null;
	}

	@Override
	public boolean isGapAllowed() {
		return false;
	}

	public ReplStage.ReplProto getReplProto() {
		return null;
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

		int maxChunk = Math.max(1, asyncWriteMaxBytes.getAsInt());
		int wrote = 0;
		while (byteBuf.isReadable()) {
			int chunkLength = Math.min(byteBuf.readableBytes(), maxChunk);
			byte[] chunk = new byte[chunkLength];
			byteBuf.readBytes(chunk);

			long flushed = AsyncFileSystemHelper.await(
					asyncFileSystem.write(writeAsyncFile, chunk, chunkLength), "write rdb " + file);
			if (flushed != chunkLength) {
				throw new IOException("short async rdb write, expected " + chunkLength + " but flushed " + flushed);
			}
			wrote += chunkLength;
		}
		return wrote;
	}

	@Override
	public void truncateEndRdb(int reduceLen) throws IOException {
		
		getLogger().info("[truncateEndRdb]{}, {}", this, reduceLen);
		
		synchronized (truncateLock) {
			long size = AsyncFileSystemHelper.await(asyncFileSystem.size(writeAsyncFile), "size rdb " + file);
			getLogger().info("[truncateEndRdb]{}, size {}->{}", this, size, size - reduceLen);
			AsyncFileSystemHelper.await(asyncFileSystem.truncate(writeAsyncFile, size - reduceLen), "truncate rdb " + file);
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
			if (writeAsyncFile != null) {
				try {
					AsyncFileSystemHelper.await(asyncFileSystem.fsync(writeAsyncFile), "fsync rdb " + file);
				} catch (IOException e) {
					getLogger().error("[endRdb][fsync]" + this, e);
				}
			}
			checkAndSetRdbState();
		}finally{
			notifyListenersEndRdb();
			closeWriteHandleQuietly();
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
		closeWriteHandleQuietly();
	}

	@Override
	public long rdbFileLength() {
		
		if(status.get() == Status.Fail){
			return FAIL_RDB_LENGTH;
		}
		try {
			return fileSize();
		} catch (IOException e) {
			getLogger().error("[rdbFileLength]" + this, e);
			return FAIL_RDB_LENGTH;
		}
	}

	private void checkAndSetRdbState() {

		try {
			long len = fileSize();
			if(eofType.fileOk(len)){
				status.set(Status.Success);
				getLogger().info("[checkAndSetRdbState]{}, {}", this, status);
			} else {
				status.set(Status.Fail);
				getLogger().error("[checkAndSetRdbState]actual:{}, expected:{}, file:{}, status:{}", len, eofType, file, status);
			}
		} catch (IOException e) {
			status.set(Status.Fail);
			getLogger().error("[checkAndSetRdbState][io]" + this, e);
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

		try {
			doReadRdbFileInfo(rdbFileListener);
			doReadRdbFile(rdbFileListener);
		} catch (Exception e) {
			getLogger().error("[readRdbFile]Error read rdb file" + file, e);
			rdbFileListener.exception(e);
		}finally{
			refCount.decrementAndGet();
		}
	}

	protected void doReadRdbFileInfo(RdbFileListener rdbFileListener) {
		if (rdbFileListener.supportProgress(BacklogOffsetReplicationProgress.class) && contiguousBacklogOffset >= 0) {
			rdbFileListener.setRdbFileInfo(eofType, new BacklogOffsetReplicationProgress(contiguousBacklogOffset));
		} else if (rdbFileListener.supportProgress(OffsetReplicationProgress.class)) {
			rdbFileListener.setRdbFileInfo(eofType, new OffsetReplicationProgress(rdbOffset));
		} else {
			throw new UnsupportedOperationException("offset progress not support");
		}
	}

	protected void doReadRdbFile(RdbFileListener rdbFileListener) throws IOException {

		AsyncFile readFile = AsyncFileSystemHelper.await(
				asyncFileSystem.open(path(), false, false, true), "open rdb for read " + file);

		long curPosition = 0;
		long lastLogTime = System.currentTimeMillis();
		try {
			while (true) {
				if (!rdbFileListener.isOpen()) break;

				Status cur = status.get();
				long readable;
				if (isRdbWriting(cur)) {
					readable = readableSize(readFile);
				} else if (cur == Status.Success) {
					readable = readableSize(readFile);
					if (curPosition >= readable) break;
				} else { // Fail
					break;
				}

				int limitBytes = rdbFileListener.getFsyncLimitPerSecond();
				long available = readable - curPosition;
				long count = available <= 0 ? 0
						: (limitBytes > 0 ? Math.min(limitBytes, available) : available);

				// emit even when count == 0 so listeners that fail-fast (or finish) are still driven,
				// mirroring the legacy ReferenceFileChannel read loop.
				ReferenceFileRegion referenceFileRegion = new AsyncRdbReferenceFileRegion(
						asyncFileSystem, readFile, curPosition, count);
				curPosition += count;
				referenceFileRegion.setTotalPos(curPosition);
				try {
					rdbFileListener.onFileData(referenceFileRegion);
				} catch (Throwable t) {
					logger.info("[doReadRdbFile] exception on send file data", t);
					referenceFileRegion.deallocate();
					throw t;
				}

				if (count <= 0) {
					try {
						Thread.sleep(1);
						long currentTime = System.currentTimeMillis();
						if (currentTime - lastLogTime > 10000) {
							getLogger().info("[doReadRdbFile]status:{}, pos:{}, rdbFileListener:{}",
									status.get(), curPosition, rdbFileListener);
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
					if (AsyncFileSystemHelper.await(asyncFileSystem.exists(path()), "exists rdb " + file)) {
						rdbFileListener.onFileData(null);
					} else {
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
		} finally {
			try {
				AsyncFileSystemHelper.await(asyncFileSystem.close(readFile), "close rdb read " + file);
			} catch (IOException e) {
				getLogger().error("[doReadRdbFile][close]" + file, e);
			}
		}
	}

	// readable size visible to a streaming reader; while writing an EofMark rdb the trailing
	// mark length is hidden (mirrors the legacy SizeControllableFile behaviour).
	private long readableSize(AsyncFile readFile) throws IOException {
		if (status.get() == Status.Writing && eofType instanceof EofMarkType) {
			long realSize;
			synchronized (truncateLock) {//truncate may make size wrong
				realSize = AsyncFileSystemHelper.await(asyncFileSystem.size(readFile), "size rdb read " + file);
			}
			long ret = realSize - ((EofMarkType) eofType).getTag().length();
			return ret < 0 ? 0 : ret;
		}
		return AsyncFileSystemHelper.await(asyncFileSystem.size(readFile), "size rdb read " + file);
	}

	private long fileSize() throws IOException {
		AsyncFile h = metadataAsyncFile();
		if (h == null) {
			return 0L;
		}
		return AsyncFileSystemHelper.await(asyncFileSystem.size(h), "size rdb " + file);
	}

	private long getRdbFileLastModifiedViaFs() throws IOException {
		AsyncFile h = metadataAsyncFile();
		if (h == null) {
			return 0L;
		}
		return AsyncFileSystemHelper.await(asyncFileSystem.lastModified(h), "lastModified rdb " + file);
	}

	// prefer live write handle while dumping; otherwise reuse cached/lazy read handle (no per-call open/close)
	private AsyncFile metadataAsyncFile() throws IOException {
		synchronized (handleLock) {
			makeSureOpen();
			AsyncFile h = writeAsyncFile;
			if (h != null) {
				return h;
			}
			h = readAsyncFile;
			if (h != null) {
				return h;
			}
			if (!AsyncFileSystemHelper.await(asyncFileSystem.exists(path()), "exists rdb " + file)) {
				return null;
			}
			readAsyncFile = openReadHandle();
			return readAsyncFile;
		}
	}

	private AsyncFile openWriteHandle() throws IOException {
		return AsyncFileSystemHelper.await(
				asyncFileSystem.open(path(), true, false, true), "open rdb for write " + file);
	}

	private AsyncFile openReadHandle() throws IOException {
		return AsyncFileSystemHelper.await(
				asyncFileSystem.open(path(), false, false, true), "open rdb for read " + file);
	}

	private String path() {
		return file.getAbsolutePath();
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
		Status cur = status.get();
		if (cur == Status.Writing) {
			return true;
		}
		if (cur == Status.Success) {
			try {
				return AsyncFileSystemHelper.await(asyncFileSystem.exists(path()), "exists rdb " + file);
			} catch (IOException e) {
				getLogger().error("[checkOk]" + this, e);
				return false;
			}
		}
		return false;
	}

	@Override
	public void destroy() throws Exception {

		getLogger().info("[destroy][delete file]{}", file);
		close();
		AsyncFileSystemHelper.await(asyncFileSystem.delete(file.getAbsolutePath()),
				"delete rdb file " + file);
	}

	@Override
	public void close() throws IOException {
		
		if(cmpAndSetClosed()){
			getLogger().info("[close]{}", file);
			closeWriteHandleQuietly();
			closeReadHandleQuietly();
		}else{
			getLogger().warn("[close][already closed]{}", this);
		}
	}

	private void closeWriteHandleQuietly() {
		synchronized (handleLock) {
			AsyncFile h = writeAsyncFile;
			if (h != null) {
				writeAsyncFile = null;
				try {
					AsyncFileSystemHelper.await(asyncFileSystem.close(h), "close rdb write " + file);
				} catch (IOException e) {
					getLogger().error("[closeWriteHandle]" + this, e);
				}
			}
		}
	}

	private void closeReadHandleQuietly() {
		synchronized (handleLock) {
			AsyncFile h = readAsyncFile;
			if (h != null) {
				readAsyncFile = null;
				try {
					AsyncFileSystemHelper.await(asyncFileSystem.close(h), "close rdb read " + file);
				} catch (IOException e) {
					getLogger().error("[closeReadHandle]" + this, e);
				}
			}
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

	@Override
	public String toString() {
		return String.format("type:%s, eofType:%s, rdbOffset:%d, file:%s, status:%s",
				typeRef.get().name(), eofType, rdbOffset, file, status.get());
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
		try {
			return getRdbFileLastModifiedViaFs();
		} catch (IOException e) {
			getLogger().error("[getRdbFileLastModified]" + this, e);
			return 0L;
		}
	}
	
	protected Logger getLogger() {
		return logger;
	}

}
