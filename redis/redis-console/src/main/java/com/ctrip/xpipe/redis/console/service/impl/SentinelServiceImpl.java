package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.SentinelTbl;
import com.ctrip.xpipe.redis.console.model.SentinelTblDao;
import com.ctrip.xpipe.redis.console.model.SentinelTblEntity;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.SentinelService;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.List;
import java.util.Random;

@Service
public class SentinelServiceImpl extends AbstractConsoleService<SentinelTblDao> implements SentinelService {

	@Override
	public List<SentinelTbl> findAll() {
		return queryHandler.handleQuery(new DalQuery<List<SentinelTbl>>() {
			@Override
			public List<SentinelTbl> doQuery() throws DalException {
				return dao.findAll(SentinelTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public List<SentinelTbl> findAllWithDcName() {
		return queryHandler.handleQuery(new DalQuery<List<SentinelTbl>>() {
			@Override
			public List<SentinelTbl> doQuery() throws DalException {
				return dao.findAllWithDcName(SentinelTblEntity.READSET_SENTINEL_DC_NAME);
			}
		});
	}

	@Override
	public List<SentinelTbl> findAllByDcName(final String dcName) {
		return queryHandler.handleQuery(new DalQuery<List<SentinelTbl>>() {
			@Override
			public List<SentinelTbl> doQuery() throws DalException {
				return dao.findByDcName(dcName, SentinelTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public List<SentinelTbl> findBySentinelGroupId(long sentinelGroupId) {
		return queryHandler.handleQuery(new DalQuery<List<SentinelTbl>>() {
			@Override
			public List<SentinelTbl> doQuery() throws DalException {
				return dao.findBySentinelGroupId(sentinelGroupId, SentinelTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public List<SentinelTbl> findBySentinelGroupIdDeleted(long sentinelGroupId) {
		return queryHandler.handleQuery(new DalQuery<List<SentinelTbl>>() {
			@Override
			public List<SentinelTbl> doQuery() throws DalException {
				return dao.findBySentinelGroupIdDeleted(sentinelGroupId, SentinelTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public SentinelTbl findByIpPort(String ip, int port) {
		return queryHandler.handleQuery(new DalQuery<SentinelTbl>() {
			@Override
			public SentinelTbl doQuery() throws DalException {
				return dao.findByIpPort(ip, port, SentinelTblEntity.READSET_FULL);
			}
		});
	}

	protected SetinelTbl random(List<SetinelTbl> setinels) {

		Random random = new Random();

		int index = random.nextInt(setinels.size());

		return setinels.get(index);
	}

//	@Override
//	public Map<Long, SetinelTbl> findByShard(long shardId) {
//		List<DcClusterShardTbl> dcClusterShards = queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
//			@Override
//			public List<DcClusterShardTbl> doQuery() throws DalException {
//				return dcClusterShardTblDao.findAllByShardId(shardId, DcClusterShardTblEntity.READSET_FULL);
//			}
//		});
//
//		Map<Long, SetinelTbl> res = new HashMap<>(dcClusterShards.size());
//		for(DcClusterShardTbl dcClusterShard : dcClusterShards) {
//			SetinelTbl sentinel = queryHandler.handleQuery(new DalQuery<SetinelTbl>() {
//				@Override
//				public SetinelTbl doQuery() throws DalException {
//					return dao.findByPK(dcClusterShard.getSetinelId(), SetinelTblEntity.READSET_FULL);
//				}
//			});
//			if(null != sentinel) {
//				res.put(sentinel.getDcId(), sentinel);
//			}
//		}
//		return res;
//	}

	@Override
	public SentinelTbl insert(SentinelTbl sentinelTbl) {

		queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.insert(sentinelTbl);
			}
		});

		return sentinelTbl;
	}

//	@Override
//	public List<SetinelTbl> getAllSentinelsWithUsage() {
//		return queryHandler.handleQuery(new DalQuery<List<SetinelTbl>>() {
//			@Override
//			public List<SetinelTbl> doQuery() throws DalException {
//				return dao.findSentinelUsage(SetinelTblEntity.READSET_SENTINEL_USAGE);
//			}
//		});
//	}
//
//	@Override
//	public Map<String, SentinelUsageModel> getAllSentinelsUsage() {
//		List<SetinelTbl> sentinels = getAllSentinelsWithUsage();
//		Map<String, SentinelUsageModel> result = Maps.newHashMapWithExpectedSize(sentinels.size());
//		for(SetinelTbl sentinelTbl : sentinels) {
//			if(StringUtil.isEmpty(sentinelTbl.getSetinelAddress()))
//				continue;
//			String dcName = sentinelTbl.getDcInfo().getDcName();
//			result.putIfAbsent(dcName, new SentinelUsageModel(dcName));
//			SentinelUsageModel usage = result.get(dcName);
//			usage.addSentinelUsage(sentinelTbl.getSetinelAddress(), sentinelTbl.getShardCount());
//		}
//		return result;
//	}
//
//	@Override
//	public SentinelModel updateSentinelTblAddr(SentinelModel sentinel) {
//		SetinelTbl target = queryHandler.handleQuery(new DalQuery<SetinelTbl>() {
//			@Override
//			public SetinelTbl doQuery() throws DalException {
//				return dao.findByPK(sentinel.getId(), SetinelTblEntity.READSET_FULL);
//			}
//		});
//		if(target == null) {
//			throw new IllegalArgumentException("no sentinel found due to id: " + sentinel.getId());
//		}
//		target.setSetinelAddress(StringUtil.join(",", new Function<HostPort, String>() {
//			@Override
//			public String apply(HostPort hostPort) {
//				return hostPort.toString();
//			}
//		}, sentinel.getSentinels()));
//		queryHandler.handleUpdate(new DalQuery<Integer>() {
//			@Override
//			public Integer doQuery() throws DalException {
//				return dao.updateSentinelAddr(target, SetinelTblEntity.UPDATESET_ADDRESS);
//			}
//		});
//
//		return queryHandler.handleQuery(new DalQuery<SentinelModel>() {
//			@Override
//			public SentinelModel doQuery() throws DalException {
//				return new SentinelModel(dao.findByPK(target.getSetinelId(), SetinelTblEntity.READSET_FULL));
//			}
//		});
//	}
//
//	@Override
//	public RetMessage removeSentinelMonitor(String clusterName) {
//		ClusterTbl clusterTbl = clusterService.find(clusterName);
//		ClusterType clusterType = ClusterType.lookup(clusterTbl.getClusterType());
//		if (null != clusterType && clusterType.supportMultiActiveDC()) {
//			return RetMessage.createFailMessage("cluster type " + clusterType + " not support remove sentinel");
//		}
//
//		long activedcId = clusterService.find(clusterName).getActivedcId();
//		String dcName = dcService.getDcName(activedcId);
//		List<DcClusterShardTbl> dcClusterShardTbls = dcClusterShardService.findAllByDcCluster(dcName, clusterName);
//		for(DcClusterShardTbl dcClusterShard : dcClusterShardTbls) {
//			try {
//				removeSentinelMonitorByShard(dcName, clusterName, clusterType, dcClusterShard);
//			} catch (Exception e) {
//				return RetMessage.createFailMessage("[stl-id: " + dcClusterShard.getSetinelId() + "]" + e.getMessage());
//			}
//		}
//		return RetMessage.createSuccessMessage();
//	}
//
//	@VisibleForTesting
//	protected void removeSentinelMonitorByShard(String activeIdc, String clusterName, ClusterType clusterType, DcClusterShardTbl dcClusterShard) {
//		ShardTbl shardTbl = shardService.find(dcClusterShard.getShardId());
//		RemoveShardSentinelMonitorEvent shardEvent = new RemoveShardSentinelMonitorEvent(clusterName,
//				shardTbl.getShardName(), MoreExecutors.directExecutor());
//		shardEvent.setClusterType(clusterType);
//		shardEvent.setShardSentinels(find(dcClusterShard.getSetinelId()).getSetinelAddress());
//		shardEvent.setShardMonitorName(SentinelUtil.getSentinelMonitorName(clusterName, shardTbl.getSetinelMonitorName(), activeIdc));
//		shardEventHandler.handleShardDelete(shardEvent);
//	}

	@Override
	public void delete(long id) {
		SentinelTbl sentinelTbl = queryHandler.handleQuery(new DalQuery<SentinelTbl>() {
			@Override
			public SentinelTbl doQuery() throws DalException {
				return dao.findByPK(id, SentinelTblEntity.READSET_FULL);
			}
		});
		if (sentinelTbl == null) {
			throw new IllegalArgumentException(String.format("sentinel with id:%d not exist", id));
		}
		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.deleteSentinel(sentinelTbl,SentinelTblEntity.UPDATESET_FULL);
			}
		});
	}

	@Override
	public void reheal(long id) {
		SentinelTbl setinelTbl = queryHandler.handleQuery(new DalQuery<SentinelTbl>() {
			@Override
			public SentinelTbl doQuery() throws DalException {
				return dao.findByPK(id, SentinelTblEntity.READSET_FULL);
			}
		});
		setinelTbl.setDeleted(0);
		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateByPK(setinelTbl, SentinelTblEntity.UPDATESET_FULL);
			}
		});
	}


	@Override
	public void updateByPk(SentinelTbl sentinelTbl) {
		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateByPK(sentinelTbl, SentinelTblEntity.UPDATESET_FULL);
			}
		});
	}


//	private static class RemoveShardSentinelMonitorEvent extends AbstractShardEvent {
//
//		public RemoveShardSentinelMonitorEvent(String clusterName, String shardName, Executor executor) {
//			super(clusterName, shardName, executor);
//		}
//
//		@Override
//		public EventType getShardEventType() {
//			return EventType.DELETE;
//		}
//
//		@Override
//		protected ShardEvent getSelf() {
//			return RemoveShardSentinelMonitorEvent.this;
//		}
//	}

}
