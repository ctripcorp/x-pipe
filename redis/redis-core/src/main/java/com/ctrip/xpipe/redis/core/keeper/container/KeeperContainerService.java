package com.ctrip.xpipe.redis.core.keeper.container;

import com.ctrip.xpipe.redis.core.entity.GcOrphanStoreResult;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;

import java.util.List;

/**
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public interface KeeperContainerService {

	/** 返回当前 KeeperContainer 的实际访问 IP；容器形态会实时按 hostname 反查。 */
	String resolveAddress();

	KeeperInstanceMeta infoPort(int port);

	void addKeeper(KeeperTransMeta keeperTransMeta);
	
	void addOrStartKeeper(KeeperTransMeta keeperTransMeta);
	
	void removeKeeper(KeeperTransMeta keeperTransMeta);

	/**
	 * Like {@link #removeKeeper(KeeperTransMeta)}, but never deletes the replication store — for a
	 * shared TFS store that must outlive this keeper instance.
	 */
	void releaseKeeper(KeeperTransMeta keeperTransMeta);

	void startKeeper(KeeperTransMeta keeperTransMeta);

	void stopKeeper(KeeperTransMeta keeperTransMeta);

	List<KeeperInstanceMeta> getAllKeepers();

	/**
	 * Raw directory names (e.g. {@code repl_42}) directly under this container's TFS store root,
	 * unfiltered — no comparison against this container's own live keepers. The metaserver's orphan
	 * GC round is responsible for subtracting whatever replIds current cluster/shard meta still
	 * occupies; this is just "what's physically there" on the shared mount. Empty in BM mode (no
	 * shared root to list).
	 */
	List<String> listStoreDirNames();

	/**
	 * Fallback GC for a TFS store this container no longer has a live keeper for. Never trusts the
	 * caller's "this is an orphan" judgement — re-derives it locally (not in this container's live
	 * keeper map, meta file openable, meta file not modified recently) before deleting anything.
	 * Deletes {@code store_manager_meta.properties} before the rest of the directory, so a crash
	 * mid-gc never leaves a store that still looks live. Never throws; see {@link GcOrphanStoreResult}.
	 */
	GcOrphanStoreResult gcOrphanStore(Long replId, String requestId);

	/**
	 * Batch form of {@link #gcOrphanStore(Long, String)} — one request/response for the whole
	 * candidate list from one orphan-GC round, instead of a network round trip per replId (a group can
	 * have dozens to low hundreds of candidates after a long-idle shared mount). Every replId still
	 * gets its own independent safety checks and its own entry in the result list in the same order;
	 * this only batches the transport, not the decision.
	 */
	List<GcOrphanStoreResult> gcOrphanStores(List<Long> replIds, String requestId);
}
