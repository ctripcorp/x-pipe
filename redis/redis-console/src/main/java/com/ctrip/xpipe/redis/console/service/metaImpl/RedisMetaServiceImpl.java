package com.ctrip.xpipe.redis.console.service.metaImpl;

import com.ctrip.xpipe.redis.console.constant.XpipeConsoleConstant;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.console.service.notifier.ClusterMetaModifiedNotifier;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaQueryVO;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
@Service("redisMetaService")
public class RedisMetaServiceImpl extends AbstractMetaService implements RedisMetaService{
	public static long REDIS_MASTER_NULL = 0L;
	
	@Autowired
	private RedisService redisService;
	@Autowired
	private ClusterService clusterService;
	@Autowired
	private DcService dcService;
	@Autowired
	private ClusterMetaModifiedNotifier notifier;

	@Override
	public String encodeRedisAddress(RedisTbl redisTbl) {
		if(null == redisTbl) {
			return XpipeConsoleConstant.DEFAULT_ADDRESS;
		} else {
			StringBuilder sb = new StringBuilder(30);
			sb.append(redisTbl.getRedisIp());
			sb.append(":");
			sb.append(String.valueOf(redisTbl.getRedisPort()));
			return sb.toString();
		}
	}
	
	@Override
	public RedisMeta loadRedisMeta(ShardMeta shardMeta, RedisTbl redisTbl, DcMetaQueryVO dcMetaQueryVO) {
		RedisMeta redisMeta = new RedisMeta();
		
		if(null != redisTbl) {
			redisMeta.setId(redisTbl.getRunId());
			redisMeta.setIp(redisTbl.getRedisIp());
			redisMeta.setPort(redisTbl.getRedisPort());
			if(redisTbl.getRedisMaster() == REDIS_MASTER_NULL) {
				redisMeta.setMaster("");
			} else {
				if(dcMetaQueryVO.getRedisInfo().containsKey(redisTbl.getRedisMaster())) {
					redisMeta.setMaster(encodeRedisAddress(dcMetaQueryVO.getRedisInfo().get(redisTbl.getRedisMaster())));
				} else {
					for(RedisTbl redis : dcMetaQueryVO.getAllActiveKeepers().values()) {
						if (redis.getId() == redisTbl.getRedisMaster()) {
							redisMeta.setMaster(encodeRedisAddress(redis));
							break;
						}
					}
				}
			}
		}
		
		redisMeta.setParent(shardMeta);
		return redisMeta;
	}

	@Override
	public KeeperMeta loadKeeperMeta(ShardMeta shardMeta, RedisTbl redisTbl, DcMetaQueryVO dcMetaQueryVO) {
		KeeperMeta keeperMeta = new KeeperMeta();
		
		if(null != redisTbl) {
			keeperMeta.setId(redisTbl.getRunId());
			keeperMeta.setIp(redisTbl.getRedisIp());
			keeperMeta.setPort(redisTbl.getRedisPort());
			if(redisTbl.getRedisMaster() == REDIS_MASTER_NULL) {
				keeperMeta.setMaster("");
			} else {
				if(dcMetaQueryVO.getRedisInfo().containsKey(redisTbl.getRedisMaster())) {
					keeperMeta.setMaster(encodeRedisAddress(dcMetaQueryVO.getRedisInfo().get(redisTbl.getRedisMaster())));
				} else {
					for(RedisTbl redis : dcMetaQueryVO.getAllActiveKeepers().values()) {
						if(redis.getId() == redisTbl.getRedisMaster()) {
							keeperMeta.setMaster(encodeRedisAddress(redis));
							break;
						}
					}
				}
			}
			keeperMeta.setActive(redisTbl.isKeeperActive());
			keeperMeta.setKeeperContainerId(redisTbl.getKeepercontainerId());
		}
		
		keeperMeta.setParent(shardMeta);
		return keeperMeta;
	}

	@Override
	public RedisMeta getRedisMeta(ShardMeta shardMeta, RedisTbl redisInfo, Map<Long,RedisTbl> redises) {
		RedisMeta redisMeta = new RedisMeta();
		
		if(null != redisInfo) {
			redisMeta.setId(redisInfo.getRunId());
			redisMeta.setIp(redisInfo.getRedisIp());
			redisMeta.setPort(redisInfo.getRedisPort());
			if(redisInfo.getRedisMaster() == REDIS_MASTER_NULL) {
				redisMeta.setMaster("");
			} else {
				if(null != redises.get(redisInfo.getRedisMaster())) {
					redisMeta.setMaster(encodeRedisAddress(redises.get(redisInfo.getRedisMaster())));
				} else {
					redisMeta.setMaster(encodeRedisAddress(redisService.load(redisInfo.getRedisMaster())));
				}
			}
		}
		
		redisMeta.setParent(shardMeta);
		return redisMeta;
	}

	@Override
	public KeeperMeta getKeeperMeta(ShardMeta shardMeta, RedisTbl redisInfo, Map<Long,RedisTbl> redises) {
		KeeperMeta keeperMeta = new KeeperMeta();
		
		if(null != redisInfo) {
			keeperMeta.setId(redisInfo.getRunId());
			keeperMeta.setIp(redisInfo.getRedisIp());
			keeperMeta.setPort(redisInfo.getRedisPort());
			if(redisInfo.getRedisMaster() == REDIS_MASTER_NULL) {
				keeperMeta.setMaster("");
			} else {
				if(null != redises.get(redisInfo.getRedisMaster())) {
					keeperMeta.setMaster(encodeRedisAddress(redises.get(redisInfo.getRedisMaster())));
				} else {
					keeperMeta.setMaster(encodeRedisAddress(redisService.load(redisInfo.getRedisMaster())));
				}
			}
			keeperMeta.setActive(redisInfo.isKeeperActive());
			keeperMeta.setKeeperContainerId(redisInfo.getKeepercontainerId());
		}
		
		keeperMeta.setParent(shardMeta);
		
		return keeperMeta;
	}

	@Override
	public void updateKeeperStatus(String dcId, String clusterId, String shardId, KeeperMeta newActiveKeeper) {

		List<RedisTbl> keepers = RedisService.findWithRole(redisService.findShardRedises(dcId, clusterId, shardId), XpipeConsoleConstant.ROLE_KEEPER); 
		if (CollectionUtils.isEmpty(keepers)){
			return;
		}

		RedisTbl newActiveKeeperTbl = null;
		for (RedisTbl keeper: keepers){
			if (keeper.getKeepercontainerId() == newActiveKeeper.getKeeperContainerId()){
				newActiveKeeperTbl = keeper;
				break;
			}
		}
		if (newActiveKeeperTbl == null){
			RedisTbl defaultRedisTbl = new RedisTbl().setId(RedisService.MASTER_REQUIRED).setRedisIp(newActiveKeeper.getIp()).setRedisPort(newActiveKeeper.getPort())
					.setKeepercontainerId(newActiveKeeper.getKeeperContainerId()).setRunId(newActiveKeeper.getId());
			logger.warn("No exist new active keeper for {},replace with default {}",newActiveKeeperTbl, defaultRedisTbl);
			newActiveKeeperTbl = defaultRedisTbl;
			
		}

		DcTbl dcTbl = dcService.load(dcId);
		ClusterTbl clusterTbl = clusterService.load(clusterId);
		if (clusterTbl == null || dcTbl == null){
			throw new BadRequestException("Dc or Cluster not exist");
		}

		if (clusterTbl.getActivedcId() == dcTbl.getId()){ //master dc
			RedisTbl masterRedis = RedisService.findMaster(redisService.findShardRedises(dcId, clusterId, shardId)); 
			if (masterRedis == null){
				throw new IllegalStateException("shard has no master redis.");
			}
			updateKeepers(keepers, newActiveKeeper, newActiveKeeperTbl, masterRedis.getId());

			//send message to all other dc meta server
			List<DcTbl> clusterDcs = dcService.findClusterRelatedDc(clusterTbl.getClusterName());
			List<DcTbl> slaveDcs = new LinkedList<>();
			for (DcTbl dc: clusterDcs){
				if (!dc.getDcName().equals(dcTbl.getDcName())){
					slaveDcs.add(dc);
					
					// Update slave dcs' active keeper's master
					RedisTbl backupDcActiveKeeper = RedisService.findActiveKeeper(redisService.findShardRedises(dc.getDcName(), clusterId, shardId));
					if(null != backupDcActiveKeeper) {
						backupDcActiveKeeper.setRedisMaster(newActiveKeeperTbl.getId());
						redisService.updateByPK(backupDcActiveKeeper);
					}
					
				}
			}
			notifier.notifyUpstreamChanged(clusterId, shardId, newActiveKeeperTbl.getRedisIp(), newActiveKeeperTbl.getRedisPort(), slaveDcs);
			
		}else {

			DcTbl masterDc = dcService.load(clusterTbl.getActivedcId());
			RedisTbl masterDcActiveKeeper = RedisService.findActiveKeeper(redisService.findShardRedises(masterDc.getDcName(), clusterId, shardId)); 

			if(null != masterDcActiveKeeper) {
				updateKeepers(keepers, newActiveKeeper, newActiveKeeperTbl, masterDcActiveKeeper.getId());
			} else {
				updateKeepers(keepers, newActiveKeeper, newActiveKeeperTbl, RedisService.MASTER_REQUIRED_TAG);
			}
		}

	}

	private void updateKeepers(List<RedisTbl> keepers, KeeperMeta newActiveKeeper, RedisTbl newActiveKeeperTbl, long newMasterRedisId){
		for (RedisTbl keeper: keepers){
			if (keeper.getKeepercontainerId() == newActiveKeeper.getKeeperContainerId()){//new active keeper
				keeper.setKeeperActive(true);
				keeper.setRedisMaster(newMasterRedisId);
			}else {
				keeper.setKeeperActive(false);
				keeper.setRedisMaster(newActiveKeeperTbl.getId());
			}
		}
		redisService.batchUpdate(keepers);
	}

}
