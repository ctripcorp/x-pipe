package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.core.store.ReplStage;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 *         May 23, 2016
 */
public class DefaultKeeperRepl implements KeeperRepl {

	// replicationStore may be null: a read-only store without a cmd chain is not kept, and a store can be
	// released between a sync handler's doHandle and its executor. Null stages make the caller take its
	// existing "replicationstore fresh" branch instead of NPE.
	@Override
	public ReplStage preStage() {
		return replicationStore == null ? null : replicationStore.getMetaStore().getPreReplStage();
	}

	@Override
	public ReplStage currentStage() {
		return replicationStore == null ? null : replicationStore.getMetaStore().getCurrentReplStage();
	}

	private ReplicationStore replicationStore;

	public DefaultKeeperRepl(ReplicationStore replicationStore) {

		this.replicationStore = replicationStore;
	}

	@Override
	public long backlogBeginOffset() {
		return replicationStore.backlogBeginOffset();
	}

	@Override
	public long backlogEndOffset() {
		return replicationStore.backlogEndOffset();
	}

	@Override
	public long getBeginOffset() {
		return replicationStore.firstAvailableOffset();
	}

	@Override
	public long getEndOffset() {
		return replicationStore == null ? ReplicationStoreMeta.DEFAULT_END_OFFSET : replicationStore.getCurReplStageReplOff();
	}

	@Override
	public String replId() {
		//TODO remove
		if (replicationStore.getMetaStore().getCurReplStageReplId() == null) return replicationStore.getMetaStore().getReplId();
		return replicationStore.getMetaStore().getCurReplStageReplId();
	}

	@Override
	public String replId2() {
		ReplStage curReplStage = replicationStore.getMetaStore().getCurrentReplStage();
		if (curReplStage == null) {
			return replicationStore.getMetaStore().getReplId2();
		}
		return curReplStage.getReplId2();
	}

	@Override
	public Long secondReplIdOffset() {
		return replicationStore.getMetaStore().getSecondReplIdOffset();
	}

	@Override
	public GtidSet getEndGtidSet() throws IOException {
		return replicationStore.getEndGtidSet();
	}

	@Override
	public String toString() {
		return String.format("beginOffset:%d, endOffset:%d, replId:%s, replId2:%s, secondReplIdOffset:%d",
				getBeginOffset(), getEndOffset(), replId(), replId2(), secondReplIdOffset());
	}
}
