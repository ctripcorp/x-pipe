package com.ctrip.xpipe.redis.console.service.impl;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.utils.StringUtil;
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
import org.unidal.tuple.Pair;


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
	
	private Comparator<RedisTbl> redisComparator = new Comparator<RedisTbl>() {
		@Override
		public int compare(RedisTbl o1, RedisTbl o2) {
			if (o1.getId() == o2.getId()) {
				return 0;
			}
			return -1;
		}
	};
	
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
	public RedisTbl findWithIpPort(String ip, int port) {
		return queryHandler.handleQuery(new DalQuery<RedisTbl>() {
			@Override
			public RedisTbl doQuery() throws DalException {
				return dao.findWithIpPort(ip, port, RedisTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public List<RedisTbl> findAllByDcClusterShard(final long dcClusterShardId) {
		return queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
			@Override
			public List<RedisTbl> doQuery() throws DalException {
				return dao.findAllByDcClusterShardId(dcClusterShardId, null, RedisTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public List<RedisTbl> findAllByDcClusterShard(String dcId, String clusterId, String shardId) throws ResourceNotFoundException {

		return doFindAllByDcClusterShard(dcId, clusterId, shardId, null);
	}

	@Override
	public List<RedisTbl> findRedisesByDcClusterShard(String dcId, String clusterId, String shardId) throws ResourceNotFoundException {
		return doFindAllByDcClusterShard(dcId, clusterId, shardId, XpipeConsoleConstant.ROLE_REDIS);
	}

	@Override
	public List<RedisTbl> findKeepersByDcClusterShard(String dcId, String clusterId, String shardId) throws ResourceNotFoundException {
		return doFindAllByDcClusterShard(dcId, clusterId, shardId, XpipeConsoleConstant.ROLE_KEEPER);
	}


	private List<RedisTbl> doFindAllByDcClusterShard(String dcId, String clusterId, String shardId, String redisRole) throws ResourceNotFoundException {
		final DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find(dcId, clusterId, shardId);
		if (dcClusterShardTbl == null) {
			throw new ResourceNotFoundException(dcId, clusterId, shardId);
		}
		return doFindAllByDcClusterShardId(dcClusterShardTbl.getDcClusterShardId(), redisRole);
	}

	private List<RedisTbl> doFindAllByDcClusterShardId(long dcClusterShardId, String redisRole) {

		List<RedisTbl> redisTbls = queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
			@Override
			public List<RedisTbl> doQuery() throws DalException {
				return dao.findAllByDcClusterShardId(dcClusterShardId, redisRole, RedisTblEntity.READSET_FULL);
			}
		});
		return redisTbls;
	}

	protected void insert(RedisTbl... redises) {

		queryHandler.handleQuery(new DalQuery<int[]>() {
			@Override
			public int[] doQuery() throws DalException {
				return dao.insertBatch(redises);
			}
		});


	}

	@Override
	public void insertRedises(String dcId, String clusterId, String shardId, List<Pair<String, Integer>> redisAddresses) throws DalException, ResourceNotFoundException {

		DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find(dcId, clusterId, shardId);
		if(dcClusterShardTbl == null){
			throw new ResourceNotFoundException(dcId, clusterId, shardId);
		}
		List<RedisTbl> redisTbls = doFindAllByDcClusterShardId(dcClusterShardTbl.getDcClusterShardId(), XpipeConsoleConstant.ROLE_REDIS);


		List<Pair<String, Integer>> toAdd = sub(redisAddresses, redisTbls);
		logger.info("[addRedises]{}", toAdd);
		insertRedises(dcClusterShardTbl.getDcClusterShardId(), toAdd.toArray(new Pair[0]));

		notifier.notifyClusterUpdate(dcId, clusterId);
	}

	@Override
	public void deleteRedises(String dcId, String clusterId, String shardId, List<Pair<String, Integer>> redisAddresses) throws ResourceNotFoundException {

		List<RedisTbl> redisTbls = findRedisesByDcClusterShard(dcId, clusterId, shardId);
		List<RedisTbl> toDelete = inter(redisAddresses, redisTbls);
		logger.info("[deleteRedises]{}", toDelete);
		delete(toDelete.toArray(new RedisTbl[0]));

		notifier.notifyClusterUpdate(dcId, clusterId);
	}

	private RedisTbl createRedisTbl(Pair<String, Integer> addr) {
		return dao.createLocal()
				.setRedisIp(addr.getKey())
				.setRedisPort(addr.getValue())
				.setRedisRole(XpipeConsoleConstant.ROLE_REDIS)
				.setRunId("unknown");
	}

	public void delete(RedisTbl... redises) {
		queryHandler.handleQuery(new DalQuery<int[]>() {
			@Override
			public int[] doQuery() throws DalException {
				return dao.deleteBatch(redises, RedisTblEntity.UPDATESET_FULL);
			}
		});
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
	public void updateRedises(String dcName, String clusterName, String shardName, ShardModel shardModel) {
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

	private void addRedisTbl(DcClusterShardTbl dcClusterShard, List<RedisTbl> result, List<RedisTbl> redises, String defaultRole) {

		if(redises == null){
			return;
		}

		for(RedisTbl redis : redises) {
			RedisTbl proto = dao.createLocal();
			if (null != redis.getRunId()) {
				proto.setRunId(redis.getRunId());
			} else {
				proto.setRunId("unknown");
			}
			proto.setId(redis.getId()).setRedisIp(redis.getRedisIp()).setRedisPort(redis.getRedisPort())
					.setKeeperActive(redis.isKeeperActive()).setKeepercontainerId(redis.getKeepercontainerId());

			proto.setMaster(redis.isMaster()? true : false);
			if(!StringUtil.isEmpty(redis.getRedisRole())){
				proto.setRedisRole(redis.getRedisRole());
			}else {
				proto.setRedisRole(defaultRole);
			}
			if (null != dcClusterShard) {
				proto.setDcClusterShardId(dcClusterShard.getDcClusterShardId());
			}
			result.add(proto);
		}

	}


	private List<RedisTbl> formatRedisesFromShardModel(DcClusterShardTbl dcClusterShard, ShardModel shardModel) {
		List<RedisTbl> result = new LinkedList<>();
		if (null == shardModel) {
			return result;
		}

		addRedisTbl(dcClusterShard, result, shardModel.getRedises(), XpipeConsoleConstant.ROLE_REDIS);
		addRedisTbl(dcClusterShard, result, shardModel.getKeepers(), XpipeConsoleConstant.ROLE_KEEPER);
		return result;
	}

	private void validateKeeperContainers(List<RedisTbl> keepers) {
		if (2 != keepers.size()) {
			if(0 == keepers.size()) {
				return;
			}
			throw new BadRequestException("Keepers' size must be 0 or 2");
		}

		if (keepers.get(0).getKeepercontainerId() == keepers.get(1).getKeepercontainerId()) {
			throw new BadRequestException("Keepers should be assigned to different keepercontainer" + keepers);
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


	protected List<Pair<String,Integer>> sub(List<Pair<String, Integer>> userGiven, List<RedisTbl> redisTbls) {

		List<Pair<String,Integer>> result = new LinkedList<>();
		userGiven.forEach(new Consumer<Pair<String, Integer>>() {
			@Override
			public void accept(Pair<String, Integer> addr) {

				boolean exist = false;

				for(RedisTbl redisTbl : redisTbls){
					if(addr.getKey().equalsIgnoreCase(redisTbl.getRedisIp())
							&& addr.getValue().equals(redisTbl.getRedisPort())){
						exist = true;
						break;
					}
				}
				if(!exist){
					result.add(addr);
				}
			}
		});
		return  result;
	}


	protected List<RedisTbl> inter(List<Pair<String, Integer>> userGiven, List<RedisTbl> redisTbls) {

		List<RedisTbl> result = new LinkedList<>();

		redisTbls.forEach(new Consumer<RedisTbl>() {
			@Override
			public void accept(RedisTbl redisTbl) {

				boolean exist = false;

				for(Pair<String, Integer> addr : userGiven){
					if(addr.getKey().equalsIgnoreCase(redisTbl.getRedisIp())
							&& addr.getValue().equals(redisTbl.getRedisPort())){
						exist = true;
						break;
					}
				}

				if(exist){
					result.add(redisTbl);
				}

			}
		});
		return result;
	}

	public void insertRedises(long dcClusterShardId, Pair<String, Integer> ...addrs) throws DalException {

		for(Pair<String, Integer> addr : addrs){
			insert(createRedisTbl(addr).setDcClusterShardId(dcClusterShardId));
		}

	}
}
