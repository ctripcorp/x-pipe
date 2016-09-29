package com.ctrip.xpipe.redis.console.service.metaImpl;

import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.console.service.meta.ShardMetaService;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaQueryVO;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
@Service("shardMetaService")
public class ShardMetaServiceImpl extends AbstractMetaService implements ShardMetaService{
	@Autowired
	private DcService dcService;
	@Autowired
	private ClusterService clusterService;
	@Autowired
	private ShardService shardService;
	@Autowired
	private RedisService redisService;
	@Autowired
	private DcClusterService dcClusterService;
	@Autowired
	private DcClusterShardService dcClusterShardService;
	@Autowired 
	private RedisMetaService redisMetaService;
	
	@Override
	public ShardMeta loadShardMeta(ClusterMeta clusterMeta,ClusterTbl clusterTbl, ShardTbl shardTbl, DcMetaQueryVO dcMetaQueryVO) {
		ShardMeta shardMeta = new ShardMeta();
		if(null == clusterTbl || null == shardTbl) return shardMeta;
		
		shardMeta.setId(shardTbl.getShardName());
		if(clusterTbl.getActivedcId() == dcMetaQueryVO.getCurrentDc().getId()) {
			shardMeta.setUpstream("");
		} else {
			RedisTbl activeKeeper = dcMetaQueryVO.getAllActiveKeepers().get(Triple.of(clusterTbl.getActivedcId(), clusterTbl.getId(), shardTbl.getId()));
			shardMeta.setUpstream(redisMetaService.encodeRedisAddress(activeKeeper));
		}
 		shardMeta.setSetinelId(dcMetaQueryVO.getDcClusterShardMap().get(Pair.of(clusterTbl.getClusterName(), shardTbl.getShardName())).getSetinelId());
		shardMeta.setSetinelMonitorName(shardTbl.getSetinelMonitorName());
		shardMeta.setPhase(dcMetaQueryVO.getDcClusterShardMap().get(Pair.of(clusterTbl.getClusterName(), shardTbl.getShardName())).getDcClusterShardPhase());
		for(RedisTbl redis : dcMetaQueryVO.getRedisMap().get(clusterTbl.getClusterName()).get(shardTbl.getShardName())) {
			if(redis.getRedisRole().equals("keeper")) {
				shardMeta.addKeeper(redisMetaService.loadKeeperMeta(shardMeta, redis, dcMetaQueryVO));
			} else {
				shardMeta.addRedis(redisMetaService.loadRedisMeta(shardMeta, redis, dcMetaQueryVO));
			}
		}
		
		shardMeta.setParent(clusterMeta);
		return shardMeta;
	}

	@Override
	public ShardMeta getShardMeta(final String dcName, final String clusterName, final String shardName) {
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(5);
		
		Future<DcTbl> future_dcInfo = fixedThreadPool.submit(new Callable<DcTbl>() {
			@Override
			public DcTbl call() throws Exception {
				return dcService.load(dcName);
			}
		});
		Future<ClusterTbl> future_clusterInfo = fixedThreadPool.submit(new Callable<ClusterTbl>() {
			@Override
			public ClusterTbl call() throws Exception {
				return clusterService.load(clusterName);
			}
		});
		Future<ShardTbl> future_shardInfo = fixedThreadPool.submit(new Callable<ShardTbl>() {
			@Override
			public ShardTbl call() throws Exception {
				return shardService.load(clusterName, shardName);
			}
		});
		Future<DcClusterTbl> future_dcClusterInfo = fixedThreadPool.submit(new Callable<DcClusterTbl>() {
			@Override
			public DcClusterTbl call() throws Exception {
				return dcClusterService.load(dcName, clusterName);
			}
		});
		Future<DcClusterShardTbl> future_dcClusterShardInfo = fixedThreadPool.submit(new Callable<DcClusterShardTbl>() {
			@Override
			public DcClusterShardTbl call() throws Exception {
				return dcClusterShardService.load(dcName, clusterName, shardName);
			}
		});
		
		try {
			if(null == future_dcInfo.get() || null == future_clusterInfo.get() || null == future_shardInfo.get()
					|| null == future_dcClusterInfo.get() || null == future_dcClusterShardInfo.get()) {
				return new ShardMeta().setId(shardName);
			}
			return getShardMeta(future_dcInfo.get(),future_clusterInfo.get(),future_shardInfo.get(),
					future_dcClusterInfo.get(), future_dcClusterShardInfo.get());
		} catch (ExecutionException e) {
			throw new DataNotFoundException("Cannot construct shard-meta", e);
		} catch (InterruptedException e) {
			throw new ServerException("Concurrent execution failed.", e);
		} finally {
			fixedThreadPool.shutdown();
		}
	}

	private ShardMeta getShardMeta(DcTbl dcInfo, ClusterTbl clusterInfo, ShardTbl shardInfo, DcClusterTbl dcClusterInfo, DcClusterShardTbl dcClusterShardInfo) {
		ShardMeta shardMeta = new ShardMeta();
		if(null == dcInfo || null == clusterInfo || null == shardInfo || null == dcClusterInfo || null == dcClusterShardInfo) {
			return shardMeta;
		}
		
		shardMeta.setId(shardInfo.getShardName());
		if(clusterInfo.getActivedcId() == dcInfo.getId()) {
			shardMeta.setUpstream("");
		} else {
			DcClusterTbl activeDcCluster = dcClusterService.load(clusterInfo.getActivedcId(), clusterInfo.getId());
			if(null != activeDcCluster) {
				DcClusterShardTbl activeDcClusterShard = dcClusterShardService.load(activeDcCluster.getDcClusterId(), shardInfo.getId());
				if(null != activeDcClusterShard) {
					RedisTbl activeKeeper = RedisService.findActiveKeeper(redisService.findByDcClusterShardId(activeDcClusterShard.getDcClusterShardId()));
					shardMeta.setUpstream(redisMetaService.encodeRedisAddress(activeKeeper));
				}
			}
		}
		shardMeta.setSetinelId(dcClusterShardInfo.getSetinelId());
		shardMeta.setSetinelMonitorName(shardInfo.getSetinelMonitorName());
		shardMeta.setPhase(dcClusterShardInfo.getDcClusterShardPhase());
		
		List<RedisTbl> shard_redises = redisService.findByDcClusterShardId(dcClusterShardInfo.getDcClusterShardId());
		if(null != shard_redises) {
			Map<Long,RedisTbl> redises = generateRedisMap(shard_redises);
			for(RedisTbl redis : shard_redises) {
				if(redis.getRedisRole().equals("keeper")) {
					addKeeperMeta(shardMeta,redis,redises);
				} else {
					addRedisMeta(shardMeta,redis,redises);
				}
			}
		}
		
		return shardMeta;
	}
	
	@Override
	public ShardMeta encodeShardMeta(DcTbl dcInfo, ClusterTbl clusterInfo, ShardTbl shardInfo, Map<Triple<Long,Long,Long>,RedisTbl> activekeepers) {
		ShardMeta shardMeta = new ShardMeta();
		if(null == shardInfo || null == dcInfo || null == clusterInfo) return shardMeta;
		
		shardMeta.setId(shardInfo.getShardName());
		if(clusterInfo.getActivedcId() == dcInfo.getId()) {
			shardMeta.setUpstream("");
		} else {
			shardMeta.setUpstream(redisMetaService.encodeRedisAddress(
					activekeepers.get(Triple.of(clusterInfo.getActivedcId(), clusterInfo.getId(), shardInfo.getId()))
			));
		}
		shardMeta.setSetinelMonitorName(shardInfo.getSetinelMonitorName());

		try {
			DcClusterTbl dcClusterInfo = dcClusterService.load(dcInfo.getId(), clusterInfo.getId());
			DcClusterShardTbl dcClusterShardInfo = dcClusterShardService.load(dcClusterInfo.getDcClusterId(), shardInfo.getId());
			if(null == dcClusterInfo || null == dcClusterShardInfo) return shardMeta;
			
			shardMeta.setSetinelId(dcClusterShardInfo.getSetinelId());
			shardMeta.setPhase(dcClusterShardInfo.getDcClusterShardPhase());

			List<RedisTbl> shard_redises = redisService.findByDcClusterShardId(dcClusterShardInfo.getDcClusterShardId());
			if(null != shard_redises) {
				Map<Long,RedisTbl> redises = generateRedisMap(shard_redises);
				for(RedisTbl redis : shard_redises) {
					if(redis.getRedisRole().equals("keeper")) {
						addKeeperMeta(shardMeta,redis,redises);
					} else {
						addRedisMeta(shardMeta,redis,redises);
					}
				}
			}
			
			return shardMeta;
		} catch (DataNotFoundException e) {
			return shardMeta;
		}
	}
	
	private Map<Long,RedisTbl> generateRedisMap(List<RedisTbl> redises) {
		Map<Long,RedisTbl> result = new HashMap<Long,RedisTbl>(redises.size());
		if(null != redises) {
			for(RedisTbl redis : redises) {
				result.put(redis.getId(), redis);
			}
		}
		return result;
	}
	
	private void addRedisMeta(ShardMeta shardMeta, RedisTbl redisInfo, Map<Long,RedisTbl> redises) {
		shardMeta.addRedis(redisMetaService.getRedisMeta(shardMeta, redisInfo, redises));
	}
	
	private void addKeeperMeta(ShardMeta shardMeta, RedisTbl redisInfo, Map<Long, RedisTbl> redises) {
		shardMeta.addKeeper(redisMetaService.getKeeperMeta(shardMeta, redisInfo, redises));
	}

}
