package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.core.store.ReplStage;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 *         May 23, 2016
 */
public class DefaultKeeperRepl implements KeeperRepl {

	@Override
	public ReplStage preStage() {
		return replicationStore.getMetaStore().getPreReplStage();
	}

	@Override
	public ReplStage currentStage() {
		return replicationStore.getMetaStore().getCurrentReplStage();
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
		return replicationStore.getEndOffset();
	}

	@Override
	public String replId() {
		//TODO remove
		if (replicationStore.getMetaStore().getCurReplStageReplId() == null) return replicationStore.getMetaStore().getReplId();
		return replicationStore.getMetaStore().getCurReplStageReplId();
	}

	@Override
	public String replId2() {
		return replicationStore.getMetaStore().getReplId2();
	}

	@Override
	public Long secondReplIdOffset() {
		return replicationStore.getMetaStore().getSecondReplIdOffset();
	}

	@Override
	public GtidSet getBeginGtidSet() throws IOException {
		return replicationStore.getBeginGtidSet();
	}

	@Override
	public GtidSet getEndGtidSet() throws IOException {
		GtidSet end = replicationStore.getEndGtidSet();
		if (null == end) {
		    end = getBeginGtidSet();
		}
		return end;
	}

	@Override
	public boolean supportGtidSet() {
		return replicationStore.supportGtidSet();
	}

	@Override
	public String toString() {
		return String.format("beginOffset:%d, endOffset:%d, replId:%s, replId2:%s, secondReplIdOffset:%d",
				getBeginOffset(), getEndOffset(), replId(), replId2(), secondReplIdOffset());
	}
}
