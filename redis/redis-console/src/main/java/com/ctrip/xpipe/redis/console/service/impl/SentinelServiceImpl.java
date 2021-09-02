package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.notifier.EventType;
import com.ctrip.xpipe.redis.console.notifier.ShardEventHandler;
import com.ctrip.xpipe.redis.console.notifier.shard.AbstractShardEvent;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardEvent;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.Function;

@Service
public class SentinelServiceImpl extends AbstractConsoleService<SetinelTblDao> implements SentinelService {

	private DcClusterShardTblDao dcClusterShardTblDao;

	@Autowired
	private DcClusterShardService dcClusterShardService;

	@Autowired
	private ClusterService clusterService;

	@Autowired
	private DcService dcService;

	@Autowired
	private ShardEventHandler shardEventHandler;

	@Autowired
	private ShardService shardService;
	
	@PostConstruct
	private void postConstruct() {
		try {
			dcClusterShardTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterShardTblDao.class);
		} catch (ComponentLookupException e) {
			throw new ServerException("Dao construct failed.", e);
		}
	}
	
	@Override
	public List<SetinelTbl> findAllByDcName(final String dcName) {
		return queryHandler.handleQuery(new DalQuery<List<SetinelTbl>>() {
			@Override
			public List<SetinelTbl> doQuery() throws DalException {
				return dao.findByDcName(dcName, SetinelTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public Map<Long, List<SetinelTbl>> allSentinelsByDc() {

		Map<Long, List<SetinelTbl>> result = new HashMap<>();

		List<SetinelTbl> setinelTbls = queryHandler.handleQuery(new DalQuery<List<SetinelTbl>>() {
			@Override
			public List<SetinelTbl> doQuery() throws DalException {
				return dao.findAll(SetinelTblEntity.READSET_FULL);
			}
		});

		setinelTbls.forEach( setinelTbl -> {

			List<SetinelTbl> setinels = MapUtils.getOrCreate(result, setinelTbl.getDcId(), new ObjectFactory<List<SetinelTbl>>() {
				@Override
				public List<SetinelTbl> create() {
					return new LinkedList<>();
				}
			});
			setinels.add(setinelTbl);
		});

		return result;
	}

	protected SetinelTbl random(List<SetinelTbl> setinels) {

		Random random = new Random();

		int index = random.nextInt(setinels.size());

		return setinels.get(index);
	}

	@Override
	public SetinelTbl find(final long id) {
		return queryHandler.handleQuery(new DalQuery<SetinelTbl>() {
			@Override
			public SetinelTbl doQuery() throws DalException {
				return dao.findByPK(id, SetinelTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public Map<Long, SetinelTbl> findByShard(long shardId) {
		List<DcClusterShardTbl> dcClusterShards = queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dcClusterShardTblDao.findAllByShardId(shardId, DcClusterShardTblEntity.READSET_FULL);
			}
		});
		
		Map<Long, SetinelTbl> res = new HashMap<>(dcClusterShards.size());
		for(DcClusterShardTbl dcClusterShard : dcClusterShards) {
			SetinelTbl sentinel = queryHandler.handleQuery(new DalQuery<SetinelTbl>() {
				@Override
				public SetinelTbl doQuery() throws DalException {
					return dao.findByPK(dcClusterShard.getSetinelId(), SetinelTblEntity.READSET_FULL);
				}
			});
			if(null != sentinel) {
				res.put(sentinel.getDcId(), sentinel);
			}
		}
		return res;
	}

	@Override
	public SetinelTbl insert(SetinelTbl setinelTbl) {

		queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.insert(setinelTbl);
			}
		});

		return setinelTbl;
	}

	@Override
	public List<SetinelTbl> getAllSentinelsWithUsage() {
		return queryHandler.handleQuery(new DalQuery<List<SetinelTbl>>() {
			@Override
			public List<SetinelTbl> doQuery() throws DalException {
				return dao.findSentinelUsage(SetinelTblEntity.READSET_SENTINEL_USAGE);
			}
		});
	}

	@Override
	public Map<String, SentinelUsageModel> getAllSentinelsUsage() {
		List<SetinelTbl> sentinels = getAllSentinelsWithUsage();
		Map<String, SentinelUsageModel> result = Maps.newHashMapWithExpectedSize(sentinels.size());
		for(SetinelTbl sentinelTbl : sentinels) {
			if(StringUtil.isEmpty(sentinelTbl.getSetinelAddress()))
				continue;
			String dcName = sentinelTbl.getDcInfo().getDcName();
			result.putIfAbsent(dcName, new SentinelUsageModel(dcName));
			SentinelUsageModel usage = result.get(dcName);
			usage.addSentinelUsage(sentinelTbl.getSetinelAddress(), sentinelTbl.getShardCount());
		}
		return result;
	}

	@Override
	public SentinelModel updateSentinelTblAddr(SentinelModel sentinel) {
		SetinelTbl target = queryHandler.handleQuery(new DalQuery<SetinelTbl>() {
			@Override
			public SetinelTbl doQuery() throws DalException {
				return dao.findByPK(sentinel.getId(), SetinelTblEntity.READSET_FULL);
			}
		});
		if(target == null) {
			throw new IllegalArgumentException("no sentinel found due to id: " + sentinel.getId());
		}
		target.setSetinelAddress(StringUtil.join(",", new Function<HostPort, String>() {
			@Override
			public String apply(HostPort hostPort) {
				return hostPort.toString();
			}
		}, sentinel.getSentinels()));
		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateSentinelAddr(target, SetinelTblEntity.UPDATESET_ADDRESS);
			}
		});

		return queryHandler.handleQuery(new DalQuery<SentinelModel>() {
			@Override
			public SentinelModel doQuery() throws DalException {
				return new SentinelModel(dao.findByPK(target.getSetinelId(), SetinelTblEntity.READSET_FULL));
			}
		});
	}

	@Override
	public RetMessage removeSentinelMonitor(String clusterName) {
		ClusterTbl clusterTbl = clusterService.find(clusterName);
		ClusterType clusterType = ClusterType.lookup(clusterTbl.getClusterType());
		if (null != clusterType && clusterType.supportMultiActiveDC()) {
			return RetMessage.createFailMessage("cluster type " + clusterType + " not support remove sentinel");
		}

		long activedcId = clusterService.find(clusterName).getActivedcId();
		String dcName = dcService.getDcName(activedcId);
		List<DcClusterShardTbl> dcClusterShardTbls = dcClusterShardService.findAllByDcCluster(dcName, clusterName);
		for(DcClusterShardTbl dcClusterShard : dcClusterShardTbls) {
			try {
				removeSentinelMonitorByShard(dcName, clusterName, clusterType, dcClusterShard);
			} catch (Exception e) {
				return RetMessage.createFailMessage("[stl-id: " + dcClusterShard.getSetinelId() + "]" + e.getMessage());
			}
		}
		return RetMessage.createSuccessMessage();
	}

	@VisibleForTesting
	protected void removeSentinelMonitorByShard(String activeIdc, String clusterName, ClusterType clusterType, DcClusterShardTbl dcClusterShard) {
		ShardTbl shardTbl = shardService.find(dcClusterShard.getShardId());
		RemoveShardSentinelMonitorEvent shardEvent = new RemoveShardSentinelMonitorEvent(clusterName,
				shardTbl.getShardName(), MoreExecutors.directExecutor());
		shardEvent.setClusterType(clusterType);
		shardEvent.setShardSentinels(find(dcClusterShard.getSetinelId()).getSetinelAddress());
		shardEvent.setShardMonitorName(SentinelUtil.getSentinelMonitorName(clusterName, shardTbl.getSetinelMonitorName(), activeIdc));
		shardEventHandler.handleShardDelete(shardEvent);
	}

	@Override
	public void delete(long id) {
		SetinelTbl setinelTbl = dao.createLocal();
		setinelTbl.setSetinelId(id);
		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.deleteSentinel(setinelTbl, SetinelTblEntity.UPDATESET_FULL);
			}
		});
	}

	@Override
	public void reheal(long id) {
		SetinelTbl setinelTbl = queryHandler.handleQuery(new DalQuery<SetinelTbl>() {
			@Override
			public SetinelTbl doQuery() throws DalException {
				return dao.findByPK(id, SetinelTblEntity.READSET_FULL);
			}
		});
		setinelTbl.setDeleted(false);
		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateByPK(setinelTbl, SetinelTblEntity.UPDATESET_FULL);
			}
		});
	}

	private static class RemoveShardSentinelMonitorEvent extends AbstractShardEvent {

		public RemoveShardSentinelMonitorEvent(String clusterName, String shardName, Executor executor) {
			super(clusterName, shardName, executor);
		}

		@Override
		public EventType getShardEventType() {
			return EventType.DELETE;
		}

		@Override
		protected ShardEvent getSelf() {
			return RemoveShardSentinelMonitorEvent.this;
		}
	}

	@VisibleForTesting
	protected SentinelServiceImpl setDcClusterShardService(DcClusterShardService dcClusterShardService) {
		this.dcClusterShardService = dcClusterShardService;
		return this;
	}

	@VisibleForTesting
	protected SentinelServiceImpl setClusterService(ClusterService clusterService) {
		this.clusterService = clusterService;
		return this;
	}

	@VisibleForTesting
	protected SentinelServiceImpl setDcService(DcService dcService) {
		this.dcService = dcService;
		return this;
	}

	@VisibleForTesting
	protected SentinelServiceImpl setShardEventHandler(ShardEventHandler shardEventHandler) {
		this.shardEventHandler = shardEventHandler;
		return this;
	}

	@VisibleForTesting
	protected SentinelServiceImpl setShardService(ShardService shardService) {
		this.shardService = shardService;
		return this;
	}
}
