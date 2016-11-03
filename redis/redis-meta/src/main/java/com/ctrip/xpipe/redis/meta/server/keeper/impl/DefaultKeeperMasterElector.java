package com.ctrip.xpipe.redis.meta.server.keeper.impl;

import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator.ShardUpstreamChanged;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperMasterElector;

/**
 * @author wenchao.meng
 *
 *         Sep 8, 2016
 */
@Component
public class DefaultKeeperMasterElector extends AbstractCurrentMetaObserver implements KeeperMasterElector, TopElement {

	protected void shardUpstreamChanged(ShardUpstreamChanged args) {

		logger.info("[shardUpstreamChanged]{}", args);
		currentMetaManager.setKeeperMaster(args.getClusterId(), args.getShardId(), args.getFuture());
	}

	@Override
	protected void handleClusterModified(ClusterMetaComparator comparator) {

	}

	@Override
	protected void handleClusterDeleted(ClusterMeta clusterMeta) {

	}

	@Override
	protected void handleClusterAdd(ClusterMeta clusterMeta) {

	}

}
