package com.ctrip.xpipe.redis.meta.server.keeper.impl;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.LockInternals;
import org.apache.curator.framework.recipes.locks.LockInternalsSorter;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;

/**
 * @author wenchao.meng
 *
 * Aug 6, 2016
 */
public class DefaultLeaderElectAlgorithm extends AbstractLeaderElectAlgorithm{

	@Override
	public KeeperMeta select(String leaderLatchPath, List<String> children, CuratorFramework curatorFramework) throws Exception {

		if (children != null && !children.isEmpty()) {
			List<String> sortedChildren = LockInternals.getSortedChildren("latch-", sorter, children);
			String leaderId = new String(curatorFramework.getData().forPath(leaderLatchPath + "/" + sortedChildren.get(0)));

			KeeperMeta keeper = Codec.DEFAULT.decode(leaderId, KeeperMeta.class);
			keeper.setActive(true);
			return keeper;
		}
		
		return null;
	}

	
	private LockInternalsSorter sorter = new LockInternalsSorter() {
		@Override
		public String fixForSorting(String str, String lockName) {
			return StandardLockInternalsDriver.standardFixForSorting(str, lockName);
		}
	};


}
