package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;

/**
 * @author wenchao.meng
 *
 *         May 23, 2016
 */
public class DefaultKeeperRepl implements KeeperRepl {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private ReplicationStore replicationStore;

	public DefaultKeeperRepl(ReplicationStore replicationStore) {

		this.replicationStore = replicationStore;
	}

	@Override
	public long getBeginOffset() {

		return replicationStore.getKeeperBeginOffset();
	}

	@Override
	public long getEndOffset() {

		long delta = replicationStore.endOffset() - replicationStore.beginOffset();
		return replicationStore.getKeeperBeginOffset() + delta;
	}

	@Override
	public void addCommandsListener(long offset, CommandsListener commandsListener) {

		try {
			long replicationStoreOffset = offset - replicationStore.getKeeperBeginOffset();
			replicationStore.addCommandsListener(replicationStoreOffset, commandsListener);
		} catch (IOException e) {
			logger.error("[addCommandsListener]" + offset, e);
		}
	}

}
