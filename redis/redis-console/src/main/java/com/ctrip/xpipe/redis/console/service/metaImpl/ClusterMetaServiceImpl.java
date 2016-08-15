package com.ctrip.xpipe.redis.console.service.metaImpl;

import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.console.service.meta.ShardMetaService;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaQueryVO;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Triple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("clusterMetaService")
public class ClusterMetaServiceImpl implements ClusterMetaService {
	@Autowired
	private DcService dcService;
	@Autowired
	private ClusterService clusterService;
	@Autowired
	private ShardService shardService;
	@Autowired
	private DcClusterService dcClusterService;
	@Autowired
	private DcMetaService dcMetaService;
	@Autowired
	private ShardMetaService shardMetaService;
	
	@Override
	public ClusterMeta loadClusterMeta(DcMeta dcMeta, ClusterTbl clusterTbl, DcMetaQueryVO dcMetaQueryVO) {
		ClusterMeta clusterMeta = new ClusterMeta();
		
		clusterMeta.setId(clusterTbl.getClusterName());
		clusterMeta.setActiveDc(dcMetaQueryVO.getAllDcs().get(clusterTbl.getActivedcId()).getDcName());
		clusterMeta.setPhase(dcMetaQueryVO.getDcClusterMap().get(clusterTbl.getClusterName())
				.getDcClusterPhase());
		clusterMeta.setLastModifiedTime(clusterTbl.getClusterLastModifiedTime());
		clusterMeta.setParent(dcMeta);
		
		for(ShardTbl shard : dcMetaQueryVO.getShardMap().get(clusterTbl.getClusterName())) {
			clusterMeta.addShard(shardMetaService.loadShardMeta(clusterMeta,clusterTbl, shard, dcMetaQueryVO));
		}
		
		return clusterMeta;
	}
	
	@Override
	public ClusterMeta getClusterMeta(final String dcName, final String clusterName) {
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(6);
		ClusterMeta clusterMeta =  new ClusterMeta();
		
		Future<DcTbl> future_dcInfo = fixedThreadPool.submit(new Callable<DcTbl>(){
			@Override
			public DcTbl call() throws Exception {
				return dcService.load(dcName);
			}
		});
		Future<ClusterTbl> future_clusterInfo = fixedThreadPool.submit(new Callable<ClusterTbl>(){
			@Override
			public ClusterTbl call() throws Exception {
				 return clusterService.load(clusterName);
			}
		});
		Future<DcClusterTbl> future_dcClusterInfo = fixedThreadPool.submit(new Callable<DcClusterTbl>(){
			@Override
			public DcClusterTbl call() throws Exception {
				return dcClusterService.load(dcName, clusterName);
			}
		});
		Future<List<ShardTbl>> future_ShardsInfo = fixedThreadPool.submit(new Callable<List<ShardTbl>>(){
			@Override
			public List<ShardTbl> call() throws Exception {
				return shardService.loadAllByClusterName(clusterName);
			}
		});
		Future<HashMap<Triple<Long, Long, Long>, RedisTbl>> future_activekeepersInfo = fixedThreadPool.submit(new Callable<HashMap<Triple<Long, Long, Long>, RedisTbl>>(){
			@Override
			public HashMap<Triple<Long, Long, Long>, RedisTbl> call() throws Exception {
				return dcMetaService.loadAllActiveKeepers();
			}
		});
		
		
		try {
			ClusterTbl clusterInfo = future_clusterInfo.get();
			DcClusterTbl dcClusterInfo = future_dcClusterInfo.get();
			
			clusterMeta.setId(clusterInfo.getClusterName());
			clusterMeta.setActiveDc(dcService.load(clusterInfo.getActivedcId()).getDcName());
			clusterMeta.setPhase(dcClusterInfo.getDcClusterPhase());
			clusterMeta.setLastModifiedTime(clusterInfo.getClusterLastModifiedTime());
			
			DcTbl dcInfo = future_dcInfo.get();
			List<ShardTbl> shards = future_ShardsInfo.get();
			HashMap<Triple<Long, Long, Long>, RedisTbl> activekeepers = future_activekeepersInfo.get();
			for(ShardTbl shard : shards) {
				clusterMeta.addShard(shardMetaService.encodeShardMeta(dcInfo, clusterInfo, shard, activekeepers));
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

}
