package com.ctrip.xpipe.redis.keeper.store.readonly;

import com.ctrip.xpipe.redis.keeper.store.cmd.AbstractFlyingThresholdCommandReader;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * PREPARE 只读读取（m5 D9）。共用 Store 的一个只读句柄，position read 出 {@link ByteBuf}。
 * 可见尾追上则 Store close+open 再观察；仍 0 则返回 null，退避由 {@link ReadOnlyCommandStore#addCommandsListener} 做。
 * reopen / read 失败只抛异常，由 listener 持有方断链。
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
		long visible = commandStore.totalLength() - curPosition;
		if (visible <= 0) {
			commandStore.reopenAndObserve();
			visible = commandStore.totalLength() - curPosition;
			if (visible <= 0) {
				return null;
			}
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
