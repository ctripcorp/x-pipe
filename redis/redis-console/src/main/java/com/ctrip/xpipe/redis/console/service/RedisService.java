package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.constant.XpipeConsoleConstant;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.model.RedisTblEntity;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author shyin
 * 
 * Aug 20, 2016
 */
@Service
public class RedisService extends AbstractConsoleService<RedisTblDao>{
	public static long IS_MASTER = 0;
	public static long MASTER_REQUIRED = Long.MAX_VALUE;
	public static long MASTER_REQUIRED_TAG = -1;
	public static int NO_EXIST_ID = 0;

	@Autowired
	DcClusterShardService dcClusterShardService;
	@Autowired
	KeepercontainerService keepercontainerService;
	@Autowired
	RedisUpdateService redisUpdateService;

    public List<RedisTbl> findByDcClusterShardId(final long dcClusterShardId){
    	return queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
			@Override
			public List<RedisTbl> doQuery() throws DalException {
				return dao.findAllByDcClusterShardId(dcClusterShardId, RedisTblEntity.READSET_FULL);
			}
    	});
    }
    
    public RedisTbl load(final long id) {
    	return queryHandler.handleQuery(new DalQuery<RedisTbl>() {
			@Override
			public RedisTbl doQuery() throws DalException {
				return dao.findByPK(id, RedisTblEntity.READSET_FULL);
			}
    	});
    }
    
    public static List<RedisTbl> findWithRole(List<RedisTbl> redises, String role) {
    	List<RedisTbl> result = new LinkedList<RedisTbl>();
    	
    	if(null != redises) {
    		for(RedisTbl redis : redises) {
    			if(redis.getRedisRole().equals(role)) {
    				result.add(redis);
    			}
    		}
    	}
    	
    	return result;
    }
    
    public static RedisTbl findActiveKeeper(List<RedisTbl> redises) {
    	RedisTbl result = null;

		if(null != redises) {
			for(RedisTbl redis : redises) {
				if(redis.getRedisRole().equals(XpipeConsoleConstant.ROLE_KEEPER) && (redis.isKeeperActive() == true )) {
					result = redis;
					break;
				}
			}
		}

    	return result;
    }

    public static RedisTbl findMaster(List<RedisTbl> redises) {
    	int masterCnt = 0;
    	RedisTbl master = null;

		if(null != redises) {
			for(RedisTbl redis : redises) {
				if(XpipeConsoleConstant.ROLE_REDIS.equals(redis.getRedisRole())) {
					if(IS_MASTER == redis.getRedisMaster()) {
						++masterCnt;
						master = redis;
					}
				}
			}
		}

    	if(masterCnt > 2) throw new BadRequestException("Cannot have more than 2 master.");
    	return master;
    }
    
    public void updateRedises(String clusterName, String dcName, String shardName, ShardModel targetShardModel) {
    	final DcClusterShardTbl dcClusterShard = dcClusterShardService.load(dcName, clusterName, shardName);
    	if(null == targetShardModel) throw new BadRequestException("RequestBody cannot be null.");
    	if(null == dcClusterShard) throw new BadRequestException("Cannot find related dc-cluster-shard.");

		List<RedisTbl> originRedises = findByDcClusterShardId(dcClusterShard.getDcClusterShardId());
		List<RedisTbl> toUpdateRedises = formatRedises(dcClusterShard, targetShardModel);

		updateRedises(originRedises, toUpdateRedises, targetShardModel);

    }
    
    private void updateRedises(List<RedisTbl> origin, List<RedisTbl> target, ShardModel targetShardModel) {
		Comparator<RedisTbl> redisComparator = new Comparator<RedisTbl>() {
			@Override
			public int compare(RedisTbl o1, RedisTbl o2) {
				if(o1.getId() == o2.getId()) {
					return 0;
				}
				return -1;
			}
		};

		List<RedisTbl> toCreate = (List<RedisTbl>) setOperator.difference(RedisTbl.class, target, origin, redisComparator);
		List<RedisTbl> toDelete = (List<RedisTbl>) setOperator.difference(RedisTbl.class, origin, target, redisComparator);
		List<RedisTbl> left = (List<RedisTbl>) setOperator.intersection(RedisTbl.class, origin, target, redisComparator);

		UPDATE_TYPE update_type = validateUpdate(toCreate, left, targetShardModel);

		updateRedises(toCreate, toDelete, left, update_type);
	}

	private void updateRedises(List<RedisTbl> toCreate, List<RedisTbl> toDelete, List<RedisTbl> left, UPDATE_TYPE update_type) {
		switch (update_type) {
			case MASTER_DC_ORIGINAL_MASTER_CHANGED_KEEPER:
				redisUpdateService.updateWithMasterDcOriginalMasterChangedKeeper(toCreate, toDelete, left);
				break;
			case MASTER_DC_ORIGINAL_MASTER_UNCHANGED_KEEPER:
				redisUpdateService.updateWithMasterDcOriginalMasterUnchangedKeeper(toCreate, toDelete, left);
				break;
			case MASTER_DC_NEW_MASTER_CHANGED_KEEPER:
				redisUpdateService.updateWithMasterDcNewMasterChangedKeeper(toCreate, toDelete, left);
				break;
			case MASTER_DC_NEW_MASTER_UNCHANGED_KEEPER:
				redisUpdateService.updateWithMasterDcNewMasterUnchangedKeeper(toCreate, toDelete, left);
				break;
			case BACKUP_DC_CHANGED_KEEPER:
				redisUpdateService.updateWithBackupDcChangedKeeper(toCreate, toDelete, left);
				break;
			case BACKUP_DC_UNCHANGED_KEEPER:
				redisUpdateService.updateWithBackupDcUnchangedKeeper(toCreate, toDelete, left);
				break;
		}
	}

	private List<RedisTbl> formatRedises(DcClusterShardTbl dcClusterShard, ShardModel shardModel) {
			List<RedisTbl> result = new LinkedList<>();
			if(null == shardModel) return result;

			if(null != shardModel.getRedises()) {
				for(RedisTbl redis : shardModel.getRedises()) {
					RedisTbl proto = dao.createLocal();
					if(null != redis.getRunId()) {
						proto.setRunId(redis.getRunId());
					} else {
						proto.setRunId("unknown");
					}
					proto.setId(redis.getId()).setRedisIp(redis.getRedisIp()).setRedisPort(redis.getRedisPort())
						.setRedisRole(XpipeConsoleConstant.ROLE_REDIS);
					if(redis.getRedisMaster() == NO_EXIST_ID) {
						proto.setRedisMaster(IS_MASTER);
					} else if(redis.getRedisMaster() == -1){
						proto.setRedisMaster(MASTER_REQUIRED);
					} else {
						proto.setRedisMaster(redis.getRedisMaster());
					}
					if(null != dcClusterShard) {
						proto.setDcClusterShardId(dcClusterShard.getDcClusterShardId());
					}
					result.add(proto);
				}
			}

			if(null != shardModel.getKeepers()) {
				for(RedisTbl keeper : shardModel.getKeepers()) {
					RedisTbl proto = dao.createLocal();
					if(null != keeper.getRunId()){
						proto.setRunId(keeper.getRunId());
					} else {
						proto.setRunId("unknown");
					}
					proto.setId(keeper.getId()).setRedisIp(keeper.getRedisIp()).setRedisPort(keeper.getRedisPort()).setKeeperActive(keeper.isKeeperActive())
						.setKeepercontainerId(keeper.getKeepercontainerId()).setRedisRole(XpipeConsoleConstant.ROLE_KEEPER);
					if(keeper.getRedisMaster() == -1) {
						proto.setRedisMaster(MASTER_REQUIRED);
					} else {
						proto.setRedisMaster(keeper.getRedisMaster());
					}
					if(null != dcClusterShard) {
						proto.setDcClusterShardId(dcClusterShard.getDcClusterShardId());
					}
					result.add(proto);
				}
			}

			return result;
	}
	
	private UPDATE_TYPE validateUpdate(List<RedisTbl> toCreate, List<RedisTbl> left, ShardModel shardModel) {
		StringBuilder sb = new StringBuilder();
		if(isInMasterDC(shardModel)) {
			sb.append("MASTER_DC").append("_")
					.append(getToUpdateMasterType(toCreate, left)).append("_")
					.append(getToUpdateKeeperType(toCreate, left));
			return UPDATE_TYPE.valueOf(sb.toString());

		} else {
			sb.append("BACKUP_DC").append("_")
					.append(getToUpdateKeeperType(toCreate, left));
			return UPDATE_TYPE.valueOf(sb.toString());
		}
	}

	private boolean isInMasterDC(ShardModel shardModel) {
		if(null == shardModel.getUpstream() || "" == shardModel.getUpstream()) return true;
		if(XpipeConsoleConstant.DEFAULT_ADDRESS.equals(shardModel.getUpstream())) return false;
    	return true;
    }
	
	private String getToUpdateMasterType(List<RedisTbl> toCreate, List<RedisTbl> left) {
		RedisTbl toCreateMaster = findMaster(toCreate);
		RedisTbl leftMaster = findMaster(left);
		if(null == toCreateMaster && null != leftMaster) {
			return "ORIGINAL_MASTER";
		} else if(null != toCreateMaster && null == leftMaster) {
			return "NEW_MASTER";
		} else {
			throw new BadRequestException("One redis master required in active DC.");
		}
	}

	private String getToUpdateKeeperType(List<RedisTbl> toCreate, List<RedisTbl> left) {
		List<RedisTbl> target = new LinkedList<>(toCreate);
		target.addAll(left);

		List<RedisTbl> keepers = findWithRole(target, XpipeConsoleConstant.ROLE_KEEPER);
		if(!((0 == keepers.size()) || (2 == keepers.size()))) {
			throw new BadRequestException("Keepers' size must be 0 or 2");
		}
		
		if(2 == keepers.size()) {
			validateKeeperContainers(keepers);
		}

		List<RedisTbl> toCreateKeepers = findWithRole(toCreate, XpipeConsoleConstant.ROLE_KEEPER);
		if(0 == toCreateKeepers.size()) {
			return "UNCHANGED_KEEPER";
		}
		return "CHANGED_KEEPER";
	}
	
	private void validateKeeperContainers(List<RedisTbl> keepers) {
		if(2 != keepers.size()) throw new BadRequestException("Keepers' size must be 0 or 2");
		
		if(keepers.get(0).getKeepercontainerId() == keepers.get(1).getKeepercontainerId()) throw new BadRequestException("Keepers should be assigned to different keepercontainer");
		
		for(int cnt = 0 ; cnt != 2 ; ++cnt) {
			final RedisTbl keeper = keepers.get(cnt);
			KeepercontainerTbl keepercontainer = keepercontainerService.load(keeper.getKeepercontainerId());
			if(null == keepercontainer) throw new BadRequestException("Cannot find related keepercontainer");
			if(!keeper.getRedisIp().equals(keepercontainer.getKeepercontainerIp())) throw new BadRequestException("Keeper's ip should be equal to keepercontainer's ip");
			
			RedisTbl redisWithSameConfiguration = queryHandler.handleQuery(new DalQuery<RedisTbl>() {
				@Override
				public RedisTbl doQuery() throws DalException {
					return dao.findWithIpPort(keeper.getRedisIp(), keeper.getRedisPort(), RedisTblEntity.READSET_FULL);
				}
			});
			if(null != redisWithSameConfiguration && !(keeper.getId() == redisWithSameConfiguration.getId()	))
				throw new BadRequestException("Already in use for keeper's port : " + String.valueOf(redisWithSameConfiguration.getRedisPort()));
		}
		
	}

	private enum UPDATE_TYPE {
		MASTER_DC_ORIGINAL_MASTER_CHANGED_KEEPER,
		MASTER_DC_ORIGINAL_MASTER_UNCHANGED_KEEPER,
		MASTER_DC_NEW_MASTER_CHANGED_KEEPER,
		MASTER_DC_NEW_MASTER_UNCHANGED_KEEPER,
		BACKUP_DC_CHANGED_KEEPER,
		BACKUP_DC_UNCHANGED_KEEPER
	}
}
