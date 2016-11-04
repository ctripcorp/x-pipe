package com.ctrip.xpipe.redis.console.service.impl;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.xpipe.redis.console.constant.XpipeConsoleConstant;
import com.ctrip.xpipe.redis.console.dao.RedisDao;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.model.RedisTblEntity;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.notifier.ClusterMetaModifiedNotifier;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.KeepercontainerService;
import com.ctrip.xpipe.redis.console.service.RedisService;

@Service
public class RedisServiceImpl extends AbstractConsoleService<RedisTblDao> implements RedisService {

	@Autowired
	private RedisDao redisDao;
	@Autowired
	private ClusterService clusterService;
	@Autowired
	private DcClusterShardService dcClusterShardService;
	@Autowired
	private KeepercontainerService keepercontainerService;
	@Autowired
	private ClusterMetaModifiedNotifier notifier;
	
	@Override
	public RedisTbl find(final long id) {
		return queryHandler.handleQuery(new DalQuery<RedisTbl>() {
			@Override
			public RedisTbl doQuery() throws DalException {
				return dao.findByPK(id, RedisTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public List<RedisTbl> findAllByDcClusterShard(final long dcClusterShardId) {
		return queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
			@Override
			public List<RedisTbl> doQuery() throws DalException {
				return dao.findAllByDcClusterShardId(dcClusterShardId, RedisTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public List<RedisTbl> findAllByDcClusterShard(String dcId, String clusterId, String shardId) {
		final DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find(dcId, clusterId, shardId);
		if (dcClusterShardTbl == null) {
			throw new BadRequestException("DcClusterShard not exist");
		}

		List<RedisTbl> redisTbls = queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
			@Override
			public List<RedisTbl> doQuery() throws DalException {
				return dao.findAllByDcClusterShardId(dcClusterShardTbl.getDcClusterShardId(), RedisTblEntity.READSET_FULL);
			}
		});

		return redisTbls;
	}

	@Override
	public void updateByPK(final RedisTbl redis) {
		if (null != redis) {
			queryHandler.handleQuery(new DalQuery<Integer>() {
				@Override
				public Integer doQuery() throws DalException {
					dao.updateByPK(redis, RedisTblEntity.UPDATESET_FULL);
					return 0;
				}
			});
		}
	}

	@Override
	public void batchUpdate(final List<RedisTbl> redises) {
		queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				redisDao.updateBatch(redises);
				return 0;
			}
		});
	}

	@Override
	public void updateRedises(String clusterName, String dcName, String shardName, ShardModel shardModel) {
		final DcClusterShardTbl dcClusterShard = dcClusterShardService.find(dcName, clusterName, shardName);
		if (null == shardModel) {
			throw new BadRequestException("RequestBody cannot be null.");
		}
		if (null == dcClusterShard) {
			throw new BadRequestException("Cannot find related dc-cluster-shard.");
		}

		List<RedisTbl> originRedises = findAllByDcClusterShard(dcClusterShard.getDcClusterShardId());
		List<RedisTbl> toUpdateRedises = formatRedisesFromShardModel(dcClusterShard, shardModel);

		updateRedises(originRedises, toUpdateRedises);

		// update current cluster to xpipe-interested
		ClusterTbl clusterInfo = clusterService.find(clusterName);
		if (null != clusterInfo && !clusterInfo.isIsXpipeInterested()) {
			clusterInfo.setIsXpipeInterested(true);
			clusterService.update(clusterInfo);
		}

		// Notify metaserver
		notifier.notifyClusterUpdate(dcName, clusterName);
	}

	private void updateRedises(List<RedisTbl> origin, List<RedisTbl> target) {
		validateKeeperContainers(RedisDao.findWithRole(target, XpipeConsoleConstant.ROLE_KEEPER));
		
		Comparator<RedisTbl> redisComparator = new Comparator<RedisTbl>() {
			@Override
			public int compare(RedisTbl o1, RedisTbl o2) {
				if (o1.getId() == o2.getId()) {
					return 0;
				}
				return -1;
			}
		};

		List<RedisTbl> toCreate = (List<RedisTbl>) setOperator.difference(RedisTbl.class, target, origin,
				redisComparator);
		List<RedisTbl> toDelete = (List<RedisTbl>) setOperator.difference(RedisTbl.class, origin, target,
				redisComparator);
		List<RedisTbl> left = (List<RedisTbl>) setOperator.intersection(RedisTbl.class, origin, target,
				redisComparator);

		updateRedises(toCreate, toDelete, left);
	}

	private void updateRedises(final List<RedisTbl> toCreate, final List<RedisTbl> toDelete, final List<RedisTbl> left) {
		queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				redisDao.handleUpdate(toCreate, toDelete, left);
				return 0;
			}
    	});
	}
	
	private List<RedisTbl> formatRedisesFromShardModel(DcClusterShardTbl dcClusterShard, ShardModel shardModel) {
		List<RedisTbl> result = new LinkedList<>();
		if (null == shardModel) {
			return result;
		}

		if (null != shardModel.getRedises()) {
			for(RedisTbl redis : shardModel.getRedises()) {
				RedisTbl proto = dao.createLocal();
				if (null != redis.getRunId()) {
					proto.setRunId(redis.getRunId());
				} else {
					proto.setRunId("unknown");
				}
				proto.setId(redis.getId()).setRedisIp(redis.getRedisIp()).setRedisPort(redis.getRedisPort())
						.setRedisRole(XpipeConsoleConstant.ROLE_REDIS);
				if(redis.getRedisMaster() == XpipeConsoleConstant.NO_EXIST_ID) {
					proto.setMaster(true);
				}
				
				if (null != dcClusterShard) {
					proto.setDcClusterShardId(dcClusterShard.getDcClusterShardId());
				}
				result.add(proto);
			}
		}

		if (null != shardModel.getKeepers()) {
			for (RedisTbl keeper : shardModel.getKeepers()) {
				RedisTbl proto = dao.createLocal();
				if (null != keeper.getRunId()) {
					proto.setRunId(keeper.getRunId());
				} else {
					proto.setRunId("unknown");
				}
				proto.setId(keeper.getId()).setRedisIp(keeper.getRedisIp()).setRedisPort(keeper.getRedisPort())
						.setKeeperActive(keeper.isKeeperActive()).setKeepercontainerId(keeper.getKeepercontainerId())
						.setRedisRole(XpipeConsoleConstant.ROLE_KEEPER);
				
				if (null != dcClusterShard) {
					proto.setDcClusterShardId(dcClusterShard.getDcClusterShardId());
				}
				result.add(proto);
			}
		}

		return result;
	}

	private void validateKeeperContainers(List<RedisTbl> keepers) {
		if (2 != keepers.size()) {
			throw new BadRequestException("Keepers' size must be 0 or 2");
		}

		if (keepers.get(0).getKeepercontainerId() == keepers.get(1).getKeepercontainerId()) {
			throw new BadRequestException("Keepers should be assigned to different keepercontainer");
		}
		
		List<RedisTbl> originalKeepers = RedisDao.findWithRole(findAllByDcClusterShard(keepers.get(0).getDcClusterShardId()), XpipeConsoleConstant.ROLE_KEEPER);
		for (int cnt = 0; cnt != 2; ++cnt) {
			final RedisTbl keeper = keepers.get(cnt);
			KeepercontainerTbl keepercontainer = keepercontainerService.find(keeper.getKeepercontainerId());
			if (null == keepercontainer) {
				throw new BadRequestException("Cannot find related keepercontainer");
			}
			if (!keeper.getRedisIp().equals(keepercontainer.getKeepercontainerIp())) {
				throw new BadRequestException("Keeper's ip should be equal to keepercontainer's ip");
			}
			
			// port check 
			RedisTbl redisWithSameConfiguration = queryHandler.handleQuery(new DalQuery<RedisTbl>() {
				@Override
				public RedisTbl doQuery() throws DalException {
					return dao.findWithIpPort(keeper.getRedisIp(), keeper.getRedisPort(), RedisTblEntity.READSET_FULL);
				}
			});
			if (null != redisWithSameConfiguration && !(keeper.getId() == redisWithSameConfiguration.getId())) {
				throw new BadRequestException("Already in use for keeper's port : "
						+ String.valueOf(redisWithSameConfiguration.getRedisPort()));
			}
			
			// keepercontainer check
			for(RedisTbl originalKeeper : originalKeepers) {
				if(originalKeeper.getKeepercontainerId() == keeper.getKeepercontainerId() 
						&& originalKeeper.getId() != keeper.getId()) {
					throw new BadRequestException("If you wanna change keeper port in same keepercontainer,please delete it first.");
				}
			}
		}

	}
	
}
