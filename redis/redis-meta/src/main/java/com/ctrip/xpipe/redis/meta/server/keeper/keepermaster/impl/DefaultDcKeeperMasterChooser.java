package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.KeeperMasterChooserAlgorithm;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.tuple.Pair;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Nov 4, 2016
 */
public class DefaultDcKeeperMasterChooser extends AbstractKeeperMasterChooser {
	
	private MultiDcService multiDcService;
	
	private XpipeNettyClientKeyedObjectPool keyedObjectPool;
	
	private KeeperMasterChooserAlgorithm keeperMasterChooserAlgorithm;

	public DefaultDcKeeperMasterChooser(Long clusterDbId, Long shardDbId, MultiDcService multiDcService,
										DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled, XpipeNettyClientKeyedObjectPool keyedObjectPool) {
		this(clusterDbId, shardDbId, multiDcService, dcMetaCache, currentMetaManager, scheduled, keyedObjectPool, DEFAULT_KEEPER_MASTER_CHECK_INTERVAL_SECONDS);
	}

	public DefaultDcKeeperMasterChooser(Long clusterDbId, Long shardDbId, MultiDcService multiDcService,
										DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled, XpipeNettyClientKeyedObjectPool keyedObjectPool, int checkIntervalSeconds) {
		super(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, scheduled, checkIntervalSeconds);
		this.multiDcService = multiDcService;
		this.keyedObjectPool = keyedObjectPool;
	}

	@Override
	protected Pair<String, Integer> chooseKeeperMaster() {

		//TODO ayq how to ensure current dc is hetero dr or hetero downstream: sources is empty?
		/*
			if 主机房: PrimaryDcKeeperMasterChooserAlgorithm
			if 单向 备机房或 异构DR机房： BackupDcKeeperMasterChooserAlgorithm
			if 异构下游机房：HeteroDownStreamDcKeeperMasterChooserAlgorithm
		 */
		if(dcMetaCache.isCurrentDcPrimary(clusterDbId, shardDbId)){
			
			if(keeperMasterChooserAlgorithm == null || !(keeperMasterChooserAlgorithm instanceof PrimaryDcKeeperMasterChooserAlgorithm)){
				
				logger.info("[chooseKeeperMaster][current dc become primary, change algorithm]cluster_{}, shard_{}", clusterDbId, shardDbId);
				keeperMasterChooserAlgorithm = new PrimaryDcKeeperMasterChooserAlgorithm(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, keyedObjectPool, checkIntervalSeconds/2, scheduled);
			}
		}else if (ClusterType.ONE_WAY.equals(dcMetaCache.getClusterType(clusterDbId)) ||
				dcMetaCache.getBakupDcs(clusterDbId, shardDbId).contains(dcMetaCache.getCurrentDc())) {
			if(keeperMasterChooserAlgorithm == null || !(keeperMasterChooserAlgorithm instanceof BackupDcKeeperMasterChooserAlgorithm)){
				logger.info("[chooseKeeperMaster][current dc become backup, change algorithm]cluster_{}, shard_{}", clusterDbId, shardDbId);
				keeperMasterChooserAlgorithm = new BackupDcKeeperMasterChooserAlgorithm(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, multiDcService, scheduled);
			}
		}else {
			if(keeperMasterChooserAlgorithm == null || !(keeperMasterChooserAlgorithm instanceof HeteroDownStreamDcKeeperMasterChooserAlgorithm)){
				logger.info("[chooseKeeperMaster][current dc hetero downstream, change algorithm]cluster_{}, shard_{}", clusterDbId, shardDbId);
				keeperMasterChooserAlgorithm = new HeteroDownStreamDcKeeperMasterChooserAlgorithm(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, multiDcService, scheduled);
			}
		}

		return keeperMasterChooserAlgorithm.choose();
	}
	
	protected KeeperMasterChooserAlgorithm getKeeperMasterChooserAlgorithm() {
		return keeperMasterChooserAlgorithm;
	}

}
