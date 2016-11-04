package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator.ShardUpstreamChanged;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcServiceManager;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperMasterElector;
import com.ctrip.xpipe.redis.meta.server.keeper.impl.AbstractCurrentMetaObserver;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author wenchao.meng
 *
 *         Sep 8, 2016
 */
@Component
public class DefaultKeeperMasterChooserManager extends AbstractCurrentMetaObserver implements KeeperMasterElector, TopElement {
	
	@Autowired
	protected DcMetaCache dcMetaCache;
	
	@Autowired
	protected MetaServerConfig metaServerConfig;
	
	@Autowired
	private MetaServerMultiDcServiceManager metaServerMultiDcServiceManager;

	protected ScheduledExecutorService scheduled;

	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		scheduled = Executors.newScheduledThreadPool(OsUtils.getCpuCount(), XpipeThreadFactory.create("DefaultKeeperMasterChooserManager"));
	}

	
	protected void shardUpstreamChanged(ShardUpstreamChanged args) {

		logger.info("[shardUpstreamChanged]{}", args);
		currentMetaManager.setKeeperMaster(args.getClusterId(), args.getShardId(), args.getFuture());
	}

	@Override
	protected void handleClusterModified(ClusterMetaComparator comparator) {
		
		String clusterId = comparator.getCurrent().getId();
		for(ShardMeta shardMeta : comparator.getAdded()){
			addShard(clusterId, shardMeta);
		}

	}

	@Override
	protected void handleClusterDeleted(ClusterMeta clusterMeta) {
		//nothing to do
	}

	@Override
	protected void handleClusterAdd(ClusterMeta clusterMeta) {
		
		for(ShardMeta shardMeta : clusterMeta.getShards().values()){
			addShard(clusterMeta.getId(), shardMeta);
		}
		
	}
	private void addShard(String clusterId, ShardMeta shardMeta) {
		
		String shardId = shardMeta.getId();
		KeeperMasterChooser keeperMasterChooser;
		
		if(dcMetaCache.isCurrentDcPrimary(clusterId, shardId)){
			keeperMasterChooser = new PrimaryDcKeeperMasterChooser(clusterId, shardId, dcMetaCache, currentMetaManager, scheduled);
		}else{
			keeperMasterChooser = new BackupDcKeeperMasterChooser(clusterId, shardId, metaServerConfig, metaServerMultiDcServiceManager, dcMetaCache, currentMetaManager, scheduled);
		}
		
		try {
			logger.info("[addShard]{}, {}, {}", clusterId, shardId, keeperMasterChooser);
			keeperMasterChooser.start();
			//release resources
			currentMetaManager.addResource(clusterId, shardId, keeperMasterChooser);
		} catch (Exception e) {
			logger.error("[addShard]{}, {}", clusterId, shardId);
		}
	}

	@Override
	protected void doDispose() throws Exception {
		scheduled.shutdownNow();
		super.doDispose();
	}
}
