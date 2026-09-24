package com.ctrip.xpipe.redis.keeper.store.readonly;

import com.ctrip.xpipe.redis.keeper.storage.SegmentOffsetBeforeFirstException;
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
 * 可见尾追上（开相）或句柄不在（关相）都只 {@code return null}，退避由
 * {@link ReadOnlyCommandStore#addCommandsListener} 做。read 失败只抛异常，由 listener 持有方断链。
 * <p>
 * 每次 {@link #doRead} 入口**只读一次**快照引用，先做区间校验再算 {@code visible}（D49）：快照被
 * 重建（cmd 链条判非法）后，落在空洞里或新尾右边的 Reader 由此自行失效，不需要任何显式中断信号。
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
		} catch (SegmentOffsetBeforeFirstException th) {
			// 单点残留（D49）：快照比 FS 状态旧，占槽方 GC 已删掉本 offset 所在前缀 —— 只断本 Reader
			logger.error("[doRead][offset before first segment]{}", this, th);
			throw new IOException("read-only command offset " + curPosition + " before first segment", th);
		} catch (Throwable th) {
			logger.error("[doRead][fail]{}", this, th);
			if (th instanceof IOException) {
				throw (IOException) th;
			}
			throw new IOException("read-only command read fail, offset=" + curPosition, th);
		}
	}

	private ByteBuf readOnce() throws IOException {
		// 快照纯内存读（D48 ④），整个 doRead 只读一次引用 —— 区间校验与 visible 必须来自同一个快照（D49）；
		// 句柄开关只由 Watcher 两相驱动，Reader 不 reopen、不 sleep、不判相位
		ReadOnlyCmdOffsetSnapshot snapshot = commandStore.offsetSnapshot();
		requireWithinSnapshot(snapshot);

		long visible = snapshot.getObservedEnd() - curPosition;
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

	/**
	 * 快照区间校验 —— 这就是**唯一**的中断信号（D49），不需要 generation / lineage / dirty flag。
	 * <p>
	 * 段链条由 {@code AsyncSegmentFile.initFromFiles} 按连续链构造，链内恒无洞，所以「落在
	 * {@code [firstOffset, observedEnd]} 内」等价于「可以安全地连续读到 {@code observedEnd}」。
	 * 两侧都是**严格**不等号：正常追尾时 {@code curPosition == observedEnd} 合法，走
	 * {@code visible <= 0} 的退避分支。
	 * <ul>
	 * <li>左半边命中：老段掉链后快照被重建，{@code firstOffset} 跳过了本 Reader（也覆盖占槽方 GC 删前缀）；</li>
	 * <li>右半边命中：末段 size 回退 / 占槽方原地重置 —— 这条补掉了「只看 {@code visible <= 0} 会静默永久停住」。</li>
	 * </ul>
	 */
	private void requireWithinSnapshot(ReadOnlyCmdOffsetSnapshot snapshot) throws IOException {
		if (curPosition < snapshot.getFirstOffset() || curPosition > snapshot.getObservedEnd()) {
			throw new IOException("read-only command offset out of snapshot range, curPosition=" + curPosition
					+ ", firstOffset=" + snapshot.getFirstOffset()
					+ ", observedEnd=" + snapshot.getObservedEnd()
					+ ", observedAtMillis=" + snapshot.getObservedAtMillis());
		}
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
