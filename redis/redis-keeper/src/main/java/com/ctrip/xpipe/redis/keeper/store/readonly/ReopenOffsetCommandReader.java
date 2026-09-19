package com.ctrip.xpipe.redis.keeper.store.readonly;

import com.ctrip.xpipe.redis.keeper.store.cmd.AbstractFlyingThresholdCommandReader;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * PREPARE 只读读取（m5 D9 / D48）。共用 Store 的一个只读句柄，position read 出 {@link ByteBuf}。
 * <p>
 * 句柄的开关由 {@code PrepareStoreWatcher} 独占驱动，Reader **永不** open / close / reopen：
 * 可见尾追上（开相）或句柄不在（关相）都只 {@code return null}，退避与等待由
 * {@link ReadOnlyCommandStore#addCommandsListener} 做。read 失败只抛异常，由 listener 持有方断链。
 */
public class ReopenOffsetCommandReader extends AbstractFlyingThresholdCommandReader<ByteBuf> {

	private static final Logger logger = LoggerFactory.getLogger(ReopenOffsetCommandReader.class);

	/**
	 * 单次 {@code fs.read} 的堆内上限，不是 PSYNC 限速。
	 */
	public static final int READ_CHUNK_BYTES = 65536;

	private long curPosition;

	private final ReadOnlyCommandStore commandStore;

	public ReopenOffsetCommandReader(long globalPosition, ReadOnlyCommandStore commandStore, long flyingThreshold) {
		super(commandStore, flyingThreshold);
		this.curPosition = globalPosition;
		this.commandStore = Objects.requireNonNull(commandStore, "commandStore");
	}

	@Override
	protected ByteBuf doRead(long milliSeconds) throws IOException {
		commandStore.makeSureOpen();
		try {
			return readOnce();
		} catch (Throwable th) {
			logger.error("[doRead][fail]{}", this, th);
			if (th instanceof IOException) {
				throw (IOException) th;
			}
			throw new IOException("read-only command read fail, offset=" + curPosition, th);
		}
	}

	private ByteBuf readOnce() throws IOException {
		// 快照纯内存读（D48 ④）；句柄开关只由 Watcher 两相驱动，Reader 不 reopen、不 sleep
		long visible = commandStore.totalLength() - curPosition;
		if (visible <= 0) {
			return null;
		}

		long toRead = Math.min(visible, READ_CHUNK_BYTES);
		ByteBuf buf = commandStore.readAt(curPosition, toRead);
		if (buf == null || !buf.isReadable()) {
			ReferenceCountUtil.release(buf);
			return null;
		}
		curPosition += buf.readableBytes();
		return buf;
	}

	@Override
	public long getReadOffset() {
		return curPosition;
	}

	@Override
	public long getCurStartOffset() {
		return commandStore.startOffsetOf(getReadOffset());
	}

	@Override
	public void close() throws IOException {
		commandStore.removeReader(this);
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}

	@Override
	public String toString() {
		return "curStartOffset:" + getCurStartOffset() + ", readOffset:" + getReadOffset();
	}
}
