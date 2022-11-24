package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 *         May 23, 2016
 */
public class DefaultKeeperRepl implements KeeperRepl {

	private ReplicationStore replicationStore;

	public DefaultKeeperRepl(ReplicationStore replicationStore) {

		this.replicationStore = replicationStore;
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
		return replicationStore.getMetaStore().getReplId();
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
	public String toString() {
		return String.format("beginOffset:%d, endOffset:%d, replId:%s, replId2:%s, secondReplIdOffset:%d",
				getBeginOffset(), getEndOffset(), replId(), replId2(), secondReplIdOffset());
	}
}
