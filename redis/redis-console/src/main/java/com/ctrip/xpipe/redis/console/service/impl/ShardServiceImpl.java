package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.dao.ShardDao;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.entity.DcClusterEntity;
import com.ctrip.xpipe.redis.console.entity.ShardEntity;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.model.consoleportal.ShardListModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.redis.console.notifier.ClusterMetaModifiedNotifier;
import com.ctrip.xpipe.redis.console.notifier.ClusterMonitorModifiedNotifier;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardDeleteEvent;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardEvent;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardEventListener;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.repository.AzGroupRepository;
import com.ctrip.xpipe.redis.console.repository.DcClusterRepository;
import com.ctrip.xpipe.redis.console.repository.ShardRepository;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.unidal.dal.jdbc.DalException;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

@Service
public class ShardServiceImpl extends AbstractConsoleService<ShardTblDao> implements ShardService {

	@Autowired
	private DcService dcService;
	@Autowired
	private ShardDao shardDao;
	@Autowired
	private ShardRepository shardRepository;
	@Autowired
	private ClusterDao clusterDao;
	@Autowired
	private ClusterMetaModifiedNotifier notifier;

	@Autowired
	private ClusterMonitorModifiedNotifier monitorNotifier;

	@Autowired
	private DelayService delayService;

	@Autowired
	private ClusterService clusterService;

	@Autowired
	private MetaCache metaCache;

	@Autowired
	private SentinelGroupService sentinelService;

	@Autowired
	private DcClusterShardService dcClusterShardService;

	@Autowired
	private DcClusterService dcClusterService;

	@Autowired
	private List<ShardEventListener> shardEventListeners;
	@Autowired
	private SentinelBalanceService sentinelBalanceService;

	@Autowired
	private AzGroupCache azGroupCache;
	@Autowired
	private AzGroupRepository azGroupRepository;
	@Autowired
	private AzGroupClusterRepository azGroupClusterRepository;
	@Autowired
	private DcClusterRepository dcClusterRepository;

	@Resource(name = GLOBAL_EXECUTOR)
	private ExecutorService executors;

	@Autowired
	private ConsoleConfig consoleConfig;

	@VisibleForTesting
	public void setConsoleConfig(ConsoleConfig consoleConfig) {
		this.consoleConfig = consoleConfig;
	}

	@Override
	public ShardTbl find(final long shardId) {
		return queryHandler.handleQuery(new DalQuery<ShardTbl>() {
			@Override
			public ShardTbl doQuery() throws DalException {
				return dao.findByPK(shardId, ShardTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public ShardTbl find(final String clusterName, final String shardName) {
		return queryHandler.handleQuery(new DalQuery<ShardTbl>() {
			@Override
			public ShardTbl doQuery() throws DalException {
				return dao.findShard(clusterName, shardName, ShardTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public List<ShardTbl> findAllByClusterName(final String clusterName) {
		return queryHandler.handleQuery(new DalQuery<List<ShardTbl>>() {
			@Override
			public List<ShardTbl> doQuery() throws DalException {
				return dao.findAllByClusterName(clusterName, ShardTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public List<ShardTbl> findAllShardNamesByClusterName(final String clusterName) {
		return queryHandler.handleQuery(new DalQuery<List<ShardTbl>>() {
			@Override
			public List<ShardTbl> doQuery() throws DalException {
				return dao.findAllByClusterName(clusterName, ShardTblEntity.READSET_NAME);
			}
    	});
	}

	private DcClusterShardTbl generateDcClusterShardTbl(ClusterTbl clusterTbl, DcClusterTbl dcClusterTbl,
		long shardId, Map<Long, SentinelGroupModel> sentinels) {
		DcClusterShardTbl dcClusterShardTbl = new DcClusterShardTbl();
		dcClusterShardTbl.setDcClusterId(dcClusterTbl.getDcClusterId()).setShardId(shardId);
		ClusterType clusterType = ClusterType.lookup(clusterTbl.getClusterType());
		if (consoleConfig.supportSentinelHealthCheck(clusterType, clusterTbl.getClusterName())) {
			SentinelGroupModel sentinelGroupModel = sentinels == null ? null : sentinels.get(dcClusterTbl.getDcId());
			dcClusterShardTbl.setSetinelId(sentinelGroupModel == null ? 0L : sentinelGroupModel.getSentinelGroupId());
		}
		if (clusterType.isCrossDc()) {
			List<DcClusterShardTbl> allShards = dcClusterShardService.findAllDcClusterTblsByShard(shardId);
			if (allShards != null && !allShards.isEmpty())
				dcClusterShardTbl.setSetinelId(allShards.get(0).getSetinelId());
		}

		return dcClusterShardTbl;
	}

	@Override
	public synchronized ShardTbl createShard(final String clusterName, final ShardTbl shard,
											 final Map<Long, SentinelGroupModel> sentinels) {
		ShardTbl shardTbl = shardDao.createShard(clusterName, shard);
		ClusterTbl clusterTbl = clusterService.find(clusterName);

		List<AzGroupClusterEntity> azGroupClusters = azGroupClusterRepository.selectByClusterId(clusterTbl.getId());
		Set<Long> singleDcAzGroupClusterIds = azGroupClusters.stream()
			.filter(agc -> ClusterType.isSameClusterType(agc.getAzGroupClusterType(), ClusterType.SINGLE_DC))
			.map(AzGroupClusterEntity::getId)
			.collect(Collectors.toSet());

		List<DcClusterTbl> dcClusterTbls = dcClusterService.findClusterRelated(clusterTbl.getId());
		List<DcClusterShardTbl> dcClusterShardTbls = new LinkedList<>();
		for (DcClusterTbl dcClusterTbl : dcClusterTbls) {
			if (singleDcAzGroupClusterIds.contains(dcClusterTbl.getAzGroupClusterId())) {
				continue;
			}
			DcClusterShardTbl dcClusterShardTbl = generateDcClusterShardTbl(clusterTbl, dcClusterTbl, shard.getId(), sentinels);
			dcClusterShardTbls.add(dcClusterShardTbl);
		}
		dcClusterShardService.insertBatch(dcClusterShardTbls);

		return shardTbl;
	}

	@Override
	public void createRegionShard(String clusterName, String regionName, String shardName) {
		ClusterTbl cluster = clusterDao.findClusterByClusterName(clusterName);
		if (cluster == null) {
			throw new BadRequestException(String.format("Cluster: %s not exist", clusterName));
		}
		if (shardName == null || !shardName.equals(shardName.trim())) {
			throw new BadRequestException("Monitor name should be exact same with shard name");
		}
		List<ShardEntity> existShards = shardRepository.selectByClusterId(cluster.getId());
		for (ShardEntity existShard : existShards) {
			if (existShard.getShardName().trim().equalsIgnoreCase(shardName.trim())) {
				throw new BadRequestException(String.format("Shard: %s already exist", shardName));
			}
			if (existShard.getSetinelMonitorName().trim().equalsIgnoreCase(shardName.trim())) {
				throw new BadRequestException(String.format("Monitor name: %s already exist", shardName));
			}
		}
		List<AzGroupClusterEntity> azGroupClusters = azGroupClusterRepository.selectByClusterId(cluster.getId());
		if (CollectionUtils.isEmpty(azGroupClusters)) {
			throw new BadRequestException(
				String.format("Cluster: %s in DC mode, cannot create region shard", clusterName));
		}

		List<DcClusterEntity> dcClusters = dcClusterRepository.selectByClusterId(cluster.getId());
		Map<Long, List<DcClusterEntity>> azGroupClusterId2DcClustersMap = dcClusters.stream()
			.collect(Collectors.groupingBy(DcClusterEntity::getAzGroupClusterId));

		List<DcClusterShardTbl> dcClusterShardList = new ArrayList<>();
		for (AzGroupClusterEntity azGroupCluster : azGroupClusters) {
			AzGroupModel azGroup = azGroupCache.getAzGroupById(azGroupCluster.getAzGroupId());
			if (azGroup.getRegion().equalsIgnoreCase(regionName)) {
				ShardEntity shard = new ShardEntity()
					.setShardName(shardName)
					.setClusterId(cluster.getId())
					.setAzGroupClusterId(azGroupCluster.getId())
					.setSetinelMonitorName(shardName);
				shardRepository.insert(shard);
				String azGroupType = StringUtil.isEmpty(azGroupCluster.getAzGroupClusterType()) ?
					cluster.getClusterType() : azGroupCluster.getAzGroupClusterType();
				Map<Long, SentinelGroupModel> sentinelModelMap =
					sentinelBalanceService.selectMultiDcSentinels(ClusterType.lookup(azGroupType));

				List<DcClusterEntity> regionDcClusters = azGroupClusterId2DcClustersMap.get(azGroupCluster.getId());
				for (DcClusterEntity dcCluster : regionDcClusters) {
					DcClusterTbl dcClusterTbl = new DcClusterTbl();
					dcClusterTbl.setDcClusterId(dcCluster.getDcClusterId());
					dcClusterTbl.setDcId(dcCluster.getDcId());
					DcClusterShardTbl dcClusterShardTbl =
						generateDcClusterShardTbl(cluster, dcClusterTbl, shard.getId(), sentinelModelMap);
					dcClusterShardList.add(dcClusterShardTbl);
				}
			}
		}
		if (!dcClusterShardList.isEmpty()) {
			dcClusterShardService.insertBatch(dcClusterShardList);
		}
	}

	@Override
	public synchronized ShardTbl findOrCreateShardIfNotExist(String clusterName, ShardTbl shard,
															 List<DcClusterTbl> dcClusterTbls, Map<Long, SentinelGroupModel> sentinels) {

		logger.info("[findOrCreateShardIfNotExist] Begin find or create shard: {}", shard);
		String monitorName = shard.getSetinelMonitorName();

		List<ShardTbl> shards = shardDao.queryAllShardsByClusterName(clusterName);

		Set<String> monitorNames = shardDao.queryAllShardMonitorNames();

		ShardTbl dupShardTbl = null;
		if(shards != null) {
			for (ShardTbl shardTbl : shards) {
				if (shardTbl.getShardName().equals(shard.getShardName())) {
					logger.info("[findOrCreateShardIfNotExist] Shard exist as: {} for input shard: {}",
							shardTbl, shard);
					dupShardTbl = shardTbl;
				}
			}
		}

		ShardTbl shardTbl;
		if(StringUtil.isEmpty(monitorName)) {
			shardTbl = generateMonitorNameAndReturnShard(dupShardTbl, monitorNames, clusterName, shard);

		} else {
			shardTbl = compareMonitorNameAndReturnShard(dupShardTbl, monitorNames, clusterName, shard);
		}

		ClusterTbl clusterTbl = clusterService.find(clusterName);
		// create dcClusterShard in all dcClusters of cluster if dcClusterTbls is null
        if (dcClusterTbls == null) {
            dcClusterTbls = dcClusterService.findClusterRelated(clusterTbl.getId()).stream().filter(dcClusterTbl -> {
                ClusterType azGroupType =
                    azGroupClusterRepository.selectAzGroupTypeById(dcClusterTbl.getAzGroupClusterId());
                return azGroupType != ClusterType.SINGLE_DC;
            }).collect(Collectors.toList());
        }

		List<DcClusterShardTbl> dcClusterShardTbls = new LinkedList<>();
		for (DcClusterTbl dcClusterTbl : dcClusterTbls) {
			long shardId = shardTbl.getId();
			DcClusterShardTbl exits = dcClusterShardService.find(dcClusterTbl.getDcClusterId(), shardId);
			if (exits != null) continue;

			DcClusterShardTbl dcClusterShardTbl = generateDcClusterShardTbl(clusterTbl, dcClusterTbl, shardId, sentinels);
			dcClusterShardTbls.add(dcClusterShardTbl);
		}
		if(!dcClusterShardTbls.isEmpty()) {
			dcClusterShardService.insertBatch(dcClusterShardTbls);
		}

		return shardTbl;
	}

	@Override
	public void deleteShard(final String clusterName, final String shardName) {
		final ShardTbl shard = queryHandler.handleQuery(new DalQuery<ShardTbl>() {
			@Override
			public ShardTbl doQuery() throws DalException {
				return dao.findShard(clusterName, shardName, ShardTblEntity.READSET_FULL);
			}
    	});

		final ClusterTbl cluster = clusterService.find(clusterName);

    	if(null != shard && null != cluster) {
    		// Call shard event
			Map<Long, SentinelGroupModel> sentinels = sentinelService.findByShard(shard.getId());
			ShardEvent shardEvent = null;
			if(sentinels != null && !sentinels.isEmpty()) {
				 shardEvent = createShardDeleteEvent(clusterName, cluster, shardName, shard, sentinels);

			}
			try {
				shardDao.deleteShardsBatch(shard);
			} catch (Exception e) {
				throw new ServerException(e.getMessage());
			}
			if(shardEvent != null) {
				shardEvent.onEvent();
			}
		}

		clusterModifyNotify(clusterName, cluster);
	}

	@Override
	public void deleteShards(ClusterTbl cluster, List<String> shardNames) {
		if(cluster != null) {
			String clusterName = cluster.getClusterName();
			List<ShardTbl> shards = queryHandler.handleQuery(new DalQuery<List<ShardTbl>>() {
				@Override
				public List<ShardTbl> doQuery() throws DalException {
					return dao.findByShardNames(clusterName, shardNames, ShardTblEntity.READSET_NAME_AND_MONITOR_NAME);
				}
			});

			if (null != shards && !shards.isEmpty()) {
				try {
					shardDao.deleteShardsBatch(shards);
				} catch (Exception e) {
					throw new ServerException(e.getMessage());
				}

				deleteShardSentinels(shards, cluster);
				clusterModifyNotify(clusterName, cluster);
			}
		}
	}

	@Override
	public List<ShardListModel> findAllUnhealthy() {
		UnhealthyInfoModel unhealthyInfoModel = delayService.getAllUnhealthyInstance();
		UnhealthyInfoModel parallelUnhealthyInfoModel = delayService.getAllUnhealthyInstanceFromParallelService();
		if (null != parallelUnhealthyInfoModel) unhealthyInfoModel.merge(parallelUnhealthyInfoModel);

		Set<String> unhealthyClusterNames = unhealthyInfoModel.getUnhealthyClusterNames();
		if (unhealthyClusterNames.isEmpty()) return Collections.emptyList();

		Map<String, ClusterTbl> clusterMap = new HashMap<>();
		List<ShardListModel> shardModels = new ArrayList<>();
		clusterService.findAllByNames(new ArrayList<>(unhealthyClusterNames))
				.forEach(cluster -> {clusterMap.put(cluster.getClusterName(), cluster);});

		for (String clusterName : unhealthyClusterNames) {
			if (!clusterMap.containsKey(clusterName)) continue;

			ClusterTbl cluster = clusterMap.get(clusterName);
			Map<String, ShardListModel> shardMap = new HashMap<>();
			unhealthyInfoModel.getUnhealthyDcShardByCluster(clusterName).forEach(dcShard -> {
				if (!shardMap.containsKey(dcShard.getShard())) {
					ShardListModel shardModel = new ShardListModel();
					shardModel.setShardName(dcShard.getShard())
							.setActivedcId(cluster.getActivedcId())
							.setClusterType(cluster.getClusterType())
							.setClusterName(cluster.getClusterName())
							.setClusterAdminEmails(cluster.getClusterAdminEmails())
							.setClusterOrgName(cluster.getClusterOrgName())
							.setClusterDescription(cluster.getClusterDescription());
					shardMap.put(dcShard.getShard(), shardModel);
				}

				shardMap.get(dcShard.getShard()).addDc(dcShard.getDc());
			});
			shardModels.addAll(shardMap.values());
		}

		return shardModels;
	}

	@VisibleForTesting
	protected ShardDeleteEvent createShardDeleteEvent(String clusterName, ClusterTbl clusterTbl, String shardName, ShardTbl shardTbl,
												Map<Long, SentinelGroupModel> sentinelTblMap) {

		ClusterType clusterType = ClusterType.lookup(clusterTbl.getClusterType());
		ShardDeleteEvent shardDeleteEvent = new ShardDeleteEvent(clusterName, shardName, executors);
		shardDeleteEvent.setClusterType(clusterType);
		if (null != clusterType && clusterType.supportMultiActiveDC()) {
			return null;
		}

		try {
			shardDeleteEvent.setShardMonitorName(metaCache.getSentinelMonitorName(clusterName, shardTbl.getShardName()));
		} catch (Exception e) {
			logger.warn("[createClusterEvent]", e);
			long activeDcId = clusterService.find(clusterName).getActivedcId();
			String activeDcName = dcService.getDcName(activeDcId);
			shardDeleteEvent.setShardMonitorName(SentinelUtil.getSentinelMonitorName(
					clusterName, shardTbl.getSetinelMonitorName(), activeDcName));
		}
		// Splicing sentinel address as "127.0.0.1:6379,127.0.0.2:6380"
		StringBuffer sb = new StringBuffer();
		for(SentinelGroupModel sentinelGroupModel : sentinelTblMap.values()) {
			sb.append(sentinelGroupModel.getSentinelsAddressString()).append(",");
		}
		sb.deleteCharAt(sb.length() - 1);

		shardDeleteEvent.setShardSentinels(sb.toString());
		shardEventListeners.forEach(shardEventListener -> shardDeleteEvent.addObserver(shardEventListener));
		return shardDeleteEvent;
	}

	private ShardTbl generateMonitorNameAndReturnShard(ShardTbl dupShardTbl, Set<String> monitorNames,
													   String clusterName, ShardTbl shard) {
		String monitorName;
		if(dupShardTbl == null) {
			monitorName = shard.getShardName();
			if(monitorNames.contains(monitorName)) {
				logger.error("[findOrCreateShardIfNotExist] monitor name {} already exist", monitorName);
				throw new IllegalStateException(String.format("monitor name %s already exist", shard.getShardName()));
			}
			shard.setSetinelMonitorName(monitorName);
			try {
				return shardDao.insertShard(clusterName, shard);
			} catch (DalException e) {
				throw new IllegalStateException(e);
			}
		} else {
			return dupShardTbl;
		}
	}

	private ShardTbl compareMonitorNameAndReturnShard(ShardTbl dupShardTbl, Set<String> monitorNames,
													  String clusterName, ShardTbl shard) {

		String monitorName = shard.getSetinelMonitorName();
		if(dupShardTbl == null) {
			if(monitorNames.contains(monitorName)) {
				logger.error("[findOrCreateShardIfNotExist] monitor name by post already exist {}", monitorName);
				throw new IllegalArgumentException(String.format("Shard monitor name %s already exist",
						monitorName));
			} else {
				try {
					return shardDao.insertShard(clusterName,shard);
				} catch (DalException e) {
					throw new IllegalStateException(e);
				}
			}
		} else {
			if(!ObjectUtils.equals(dupShardTbl.getSetinelMonitorName(), monitorName)) {
				logger.error("[findOrCreateShardIfNotExist] shard monitor name in-consist with previous, {} -> {}",
						monitorName, dupShardTbl.getSetinelMonitorName());
				throw new IllegalArgumentException(String.format("Post shard monitor name %s diff from previous %s",
						monitorName, dupShardTbl.getSetinelMonitorName()));
			}
			return dupShardTbl;
		}
	}

	private void clusterModifyNotify(String clusterName, ClusterTbl cluster) {
		List<DcTbl> relatedDcs = dcService.findClusterRelatedDc(clusterName);
		if(null == relatedDcs || null == cluster) return;

		List<String> dcs = relatedDcs.stream().map(DcTbl::getDcName).collect(Collectors.toList());
		ClusterType clusterType = ClusterType.lookup(cluster.getClusterType());

		if (consoleConfig.shouldNotifyClusterTypes().contains(clusterType.toString()))
			notifier.notifyClusterUpdate(clusterName, dcs);
		if (clusterType.supportMigration()) {
			monitorNotifier.notifyClusterUpdate(clusterName, cluster.getClusterOrgId());
		}
	}

	@Override
	public List<ShardTbl> findAllShardByDcCluster(long dcId, long clusterId) {
		return queryHandler.handleQuery(new DalQuery<List<ShardTbl>>() {
			@Override
			public List<ShardTbl> doQuery() throws DalException {
				return dao.findAllShardByDcCluster(dcId, clusterId, ShardTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public void deleteShardSentinels(List<ShardTbl> shards, ClusterTbl clusterTbl) {
		for (ShardTbl shard : shards) {
			Map<Long, SentinelGroupModel> sentinels = sentinelService.findByShard(shard.getId());
			ShardEvent shardEvent = null;
			if (sentinels != null && !sentinels.isEmpty()) {
				shardEvent = createShardDeleteEvent(clusterTbl.getClusterName(), clusterTbl, shard.getShardName(), shard, sentinels);
			}
			if (shardEvent != null) {
				shardEvent.onEvent();
			}
		}
	}

}
