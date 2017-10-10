package com.ctrip.xpipe.redis.console.service.model.impl;

import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Service
public class ShardModelServiceImpl implements ShardModelService{
	
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
	
	@Override
	public List<ShardModel> getAllShardModel(String dcName, String clusterName) {
		List<ShardModel> shardModels = new ArrayList<ShardModel>(); 
		List<ShardTbl> shards = shardService.findAllByClusterName(clusterName);
		
		if(null != shards) {
			for(ShardTbl shard : shards) {
				shardModels.add(getShardModel(dcName, clusterName, shard.getShardName()));
			}
		}
			
		return shardModels;
	}

	@Override
	public ShardModel getShardModel(final String dcName, final String clusterName, final String shardName) {
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
		Future<ShardTbl> future_shardInfo = fixedThreadPool.submit(new Callable<ShardTbl>() {
			@Override
			public ShardTbl call() throws Exception {
				return shardService.find(clusterName, shardName);
			}
		});
		Future<DcClusterTbl> future_dcClusterInfo = fixedThreadPool.submit(new Callable<DcClusterTbl>() {
			@Override
			public DcClusterTbl call() throws Exception {
				return dcClusterService.find(dcName, clusterName);
			}
		});
		Future<DcClusterShardTbl> future_dcClusterShardInfo = fixedThreadPool.submit(new Callable<DcClusterShardTbl>() {
			@Override
			public DcClusterShardTbl call() throws Exception {
				return dcClusterShardService.find(dcName, clusterName, shardName);
			}
		});
		
		try {
			if(null == future_dcInfo.get() || null == future_clusterInfo.get() || null == future_shardInfo.get()
					|| null == future_dcClusterInfo.get() || null == future_dcClusterShardInfo.get()) {
				ShardModel res = new ShardModel();
				res.setShardTbl((new ShardTbl()).setShardName(shardName));
				return res;
			}
			return getShardModel(future_dcInfo.get(),future_clusterInfo.get(),future_shardInfo.get(),
					future_dcClusterInfo.get(), future_dcClusterShardInfo.get());
		} catch (ExecutionException e) {
			throw new DataNotFoundException("Cannot construct shard-model", e);
		} catch (InterruptedException e) {
			throw new ServerException("Concurrent execution failed.", e);
		} finally {
			fixedThreadPool.shutdown();
		}
	}
	
	private ShardModel getShardModel(DcTbl dcInfo, ClusterTbl clusterInfo, ShardTbl shardInfo, DcClusterTbl dcClusterInfo, DcClusterShardTbl dcClusterShardInfo) {
		ShardModel shardModel = new ShardModel();
		if(null == dcInfo || null == clusterInfo || null == shardInfo || null == dcClusterInfo || null == dcClusterShardInfo) {
			return shardModel;
		}

		shardModel.setShardTbl(shardInfo);
		
		List<RedisTbl> shard_redises = redisService.findAllByDcClusterShard(dcClusterShardInfo.getDcClusterShardId());
		if(null != shard_redises) {
			for(RedisTbl redis : shard_redises) {
				if(redis.getRedisRole().equals(XPipeConsoleConstant.ROLE_REDIS)) {
					shardModel.addRedis(redis);
				} else {
					shardModel.addKeeper(redis);
				}
			}
		}
		
		return shardModel;
	}

}
