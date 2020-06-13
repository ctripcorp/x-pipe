package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.meta.AbstractMetaService;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.service.meta.ShardMetaService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaQueryVO;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.google.common.base.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author shyin
 *
 *         Aug 17, 2016
 */
@Service
public class ClusterMetaServiceImpl extends AbstractMetaService implements ClusterMetaService {
	@Autowired
	private DcService dcService;
	@Autowired
	private ClusterService clusterService;
	@Autowired
	private ShardService shardService;
	@Autowired
	private DcClusterService dcClusterService;
	@Autowired
	private ShardMetaService shardMetaService;
	@Autowired
	private MigrationService migrationService;

	private static final String DC_NAME_DELIMITER = ",";

	@Override
	public ClusterMeta loadClusterMeta(DcMeta dcMeta, ClusterTbl clusterTbl, DcMetaQueryVO dcMetaQueryVO) {
		ClusterMeta clusterMeta = new ClusterMeta();
		clusterTbl.setActivedcId(getClusterMetaCurrentPrimaryDc(dcMetaQueryVO.getCurrentDc(), clusterTbl));
		
		clusterMeta.setId(clusterTbl.getClusterName());
		loadDcs(dcMetaQueryVO, clusterTbl, clusterMeta);
		clusterMeta.setParent(dcMeta);

		for (ShardTbl shard : dcMetaQueryVO.getShardMap().get(clusterTbl.getClusterName())) {
			clusterMeta.addShard(shardMetaService.loadShardMeta(clusterMeta, clusterTbl, shard, dcMetaQueryVO));
		}

		return clusterMeta;
	}

	private void loadDcs(DcMetaQueryVO dcMetaQueryVO, ClusterTbl clusterTbl, ClusterMeta clusterMeta) {
		String activeDc = null;
		List<String> backupDcs = new ArrayList<>();
		List<String> allDcs = new ArrayList<>();
		for (DcClusterTbl dcCluster : dcMetaQueryVO.getAllDcClusterMap().get(clusterTbl.getId())) {
			String dcName = dcMetaQueryVO.getAllDcs().get(dcCluster.getDcId()).getDcName();
			allDcs.add(dcName);
			if (dcCluster.getDcId() == clusterTbl.getActivedcId()) {
				activeDc = dcName;
			} else {
				backupDcs.add(dcName);
			}
		}

		if (!ClusterType.lookup(clusterTbl.getClusterType()).supportMultiActiveDC()) {
			clusterMeta.setActiveDc(activeDc);
			if (!backupDcs.isEmpty()) clusterMeta.setBackupDcs(String.join(DC_NAME_DELIMITER, backupDcs));
		} else {
			clusterMeta.setDcs(String.join(DC_NAME_DELIMITER, allDcs));
		}
	}

	@Override
	public ClusterMeta getClusterMeta(final String dcName, final String clusterName) {
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(5);

		Future<DcTbl> future_dcInfo = fixedThreadPool.submit(new Callable<DcTbl>() {
			@Override
			public DcTbl call() throws Exception {
				return dcService.find(dcName);
			}
		});
		Future<ClusterTbl> future_clusterInfo = fixedThreadPool.submit(new Callable<ClusterTbl>() {
			@Override
			public ClusterTbl call() throws Exception {
				return clusterService.find(clusterName);
			}
		});
		Future<DcClusterTbl> future_dcClusterInfo = fixedThreadPool.submit(new Callable<DcClusterTbl>() {
			@Override
			public DcClusterTbl call() throws Exception {
				return dcClusterService.find(dcName, clusterName);
			}
		});
		Future<List<ShardTbl>> future_shardsInfo = fixedThreadPool.submit(new Callable<List<ShardTbl>>() {
			@Override
			public List<ShardTbl> call() throws Exception {
				return shardService.findAllByClusterName(clusterName);
			}
		});
		Future<List<DcTbl>> future_clusterRelatedDc = fixedThreadPool.submit(new Callable<List<DcTbl>>() {
			@Override
			public List<DcTbl> call() throws Exception {
				return dcService.findClusterRelatedDc(clusterName);
			}
		});

		ClusterMeta clusterMeta = new ClusterMeta();
		clusterMeta.setId(clusterName);
		try {
			DcTbl dcInfo = future_dcInfo.get();
			ClusterTbl clusterInfo = future_clusterInfo.get();
			DcClusterTbl dcClusterInfo = future_dcClusterInfo.get();
			List<DcTbl> clusterRelatedDc = future_clusterRelatedDc.get();
			if (null == dcInfo || null == clusterInfo || null == dcClusterInfo)
				return clusterMeta;

			clusterMeta.setId(clusterInfo.getClusterName());
			clusterMeta.setType(clusterInfo.getClusterType());
			clusterInfo.setActivedcId(getClusterMetaCurrentPrimaryDc(dcInfo, clusterInfo));
			
			for (DcTbl dc : clusterRelatedDc) {
				if (dc.getId() == clusterInfo.getActivedcId()) {
					clusterMeta.setActiveDc(dc.getDcName());
				} else {
					if (Strings.isNullOrEmpty(clusterMeta.getBackupDcs())) {
						clusterMeta.setBackupDcs(dc.getDcName());
					} else {
						clusterMeta.setBackupDcs(clusterMeta.getBackupDcs() + "," + dc.getDcName());
					}
				}
			}

			List<ShardTbl> shards = future_shardsInfo.get();
			if (null != shards) {
				for (ShardTbl shard : shards) {
					clusterMeta.addShard(shardMetaService.getShardMeta(dcInfo, clusterInfo, shard));
				}
			}
		} catch (ExecutionException e) {
			throw new DataNotFoundException("Cannot construct cluster-meta", e);
		} catch (InterruptedException e) {
			throw new ServerException("Concurrent execution failed.", e);
		} finally {
			fixedThreadPool.shutdown();
		}

		return clusterMeta;
	}
	
	/** Perform differently with migrating cluster **/
	@Override
	public long getClusterMetaCurrentPrimaryDc(DcTbl dcInfo, ClusterTbl clusterInfo) {
		if (ClusterStatus.isSameClusterStatus(clusterInfo.getStatus(), ClusterStatus.Migrating)) {
			MigrationClusterTbl migrationCluster = migrationService.findLatestUnfinishedMigrationCluster(clusterInfo.getId());
			if(migrationCluster != null && dcInfo.getId() == migrationCluster.getDestinationDcId()) {
				return migrationCluster.getDestinationDcId();
			}
		}
		return clusterInfo.getActivedcId();
	}
	
	public void setMigrationService(MigrationService migrationService) {
		this.migrationService = migrationService;
	}

}
