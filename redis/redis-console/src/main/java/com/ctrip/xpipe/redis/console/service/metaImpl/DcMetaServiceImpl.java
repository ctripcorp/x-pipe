package com.ctrip.xpipe.redis.console.service.metaImpl;

import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.MetaserverTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.KeepercontainerService;
import com.ctrip.xpipe.redis.console.service.MetaserverService;
import com.ctrip.xpipe.redis.console.service.SetinelService;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.console.service.meta.KeepercontainerMetaService;
import com.ctrip.xpipe.redis.console.service.meta.MetaserverMetaService;
import com.ctrip.xpipe.redis.console.service.meta.SetinelMetaService;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaQueryVO;
import com.ctrip.xpipe.redis.console.util.DataModifiedTimeGenerator;
import com.ctrip.xpipe.redis.core.entity.DcMeta;

import org.apache.commons.lang3.tuple.Triple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
@Service("dcMetaService")
public class DcMetaServiceImpl extends AbstractMetaService implements DcMetaService{
	@Autowired
	private DcService dcService;
	@Autowired
	private MetaserverService metaserverService;
	@Autowired
	private SetinelService setinelService;
	@Autowired
	private KeepercontainerService keepercontainerService;
	@Autowired 
	private MetaserverMetaService metaserverMetaService;
	@Autowired 
	private SetinelMetaService setinelMetaService;
	@Autowired
	private KeepercontainerMetaService keepercontainerMetaService;
	@Autowired
	private ClusterMetaService clusterMetaService;
	
    @Override
    public DcMeta getDcMeta(final String dcName) {
    	ExecutorService fixedThreadPool = Executors.newFixedThreadPool(6);
    	DcMeta dcMeta = new DcMeta();
    	dcMeta.setId(dcName);
    	dcMeta.setLastModifiedTime(DataModifiedTimeGenerator.generateModifiedTime());
    	
    	Future<DcTbl> future_dcInfo = fixedThreadPool.submit(new Callable<DcTbl>() {
			@Override
			public DcTbl call() throws Exception {
				return dcService.load(dcName);
			}
    	});
    	Future<List<MetaserverTbl>> future_metaservers = fixedThreadPool.submit(new Callable<List<MetaserverTbl>>() {
			@Override
			public List<MetaserverTbl> call() throws Exception {
				return metaserverService.findByDcName(dcName);
			}
    	});
    	Future<List<SetinelTbl>> future_setinels = fixedThreadPool.submit(new Callable<List<SetinelTbl>>() {
			@Override
			public List<SetinelTbl> call() throws Exception {
				return setinelService.findByDcName(dcName);
			}
    	});
    	Future<List<KeepercontainerTbl>> future_keepercontainers = fixedThreadPool.submit(new Callable<List<KeepercontainerTbl>>() {
			@Override
			public List<KeepercontainerTbl> call() throws Exception {
				return keepercontainerService.findByDcName(dcName);
			}
    	});
    	Future<HashMap<Triple<Long, Long, Long>, RedisTbl>> future_allactivekeepers = fixedThreadPool.submit(new Callable<HashMap<Triple<Long, Long, Long>, RedisTbl>>() {
			@Override
			public HashMap<Triple<Long, Long, Long>, RedisTbl> call() throws Exception {
				return loadAllActiveKeepers();
			}
    	});
    	Future<HashMap<Long, DcTbl>> future_alldcs = fixedThreadPool.submit(new Callable<HashMap<Long, DcTbl>>() {
			@Override
			public HashMap<Long, DcTbl> call() throws Exception {
				return loadAllDcs();
			}
    	});
    	
    	DcTbl dcInfo;
		try {
			dcInfo = future_dcInfo.get();
			if(null == dcInfo) return dcMeta;
			
			dcMeta.setId(dcInfo.getDcName());
	    	dcMeta.setLastModifiedTime(dcInfo.getDcLastModifiedTime());
	    	
	    	if(null != future_metaservers.get()) {
	    		for(MetaserverTbl metaserver : future_metaservers.get()) {
		    		dcMeta.addMetaServer(metaserverMetaService.encodeMetaserver(metaserver, dcMeta));
		    	}
	    	}
	    	if(null != future_setinels.get()) {
	    		for(SetinelTbl setinel : future_setinels.get()) {
		    		dcMeta.addSetinel(setinelMetaService.encodeSetinelMeta(setinel, dcMeta));
		    	}
	    	}
	    	if(null != future_keepercontainers.get()) {
	    		for(KeepercontainerTbl keepercontainer : future_keepercontainers.get()) {
		    		dcMeta.addKeeperContainer(keepercontainerMetaService.encodeKeepercontainerMeta(keepercontainer, dcMeta));
		    	}
	    	}
	    	
	    	
			DcMetaQueryVO dcMetaQueryVO = loadMetaVO(dcInfo, dcService.findAllDetails(dcName));
			if(null != future_allactivekeepers.get()) {
				dcMetaQueryVO.setAllActiveKeepers(future_allactivekeepers.get());
			}
			if(null != future_alldcs.get()) {
				dcMetaQueryVO.setAllDcs(future_alldcs.get());
			}
			
			for(ClusterTbl cluster : dcMetaQueryVO.getClusterInfo().values()){
				dcMeta.addCluster(clusterMetaService.loadClusterMeta(dcMeta, cluster, dcMetaQueryVO));
			}
		} catch (ExecutionException e) {
			throw new DataNotFoundException("Cannot construct dc-meta", e);
		} catch (InterruptedException e) {
			throw new ServerException("Concurrent execution failed.", e);
		} finally {
			fixedThreadPool.shutdown();
		}
    	return dcMeta;
    }

    @Override
	public HashMap<Triple<Long, Long, Long>, RedisTbl> loadAllActiveKeepers() {
		HashMap<Triple<Long, Long, Long>, RedisTbl> results = new HashMap<>();
		
		List<DcTbl> allActiveKeepers = dcService.findAllActiveKeepers();
		if(null != allActiveKeepers) {
			for (DcTbl dcTbl : dcService.findAllActiveKeepers()) {
				if(! results.containsKey(Triple.of(dcTbl.getId(), dcTbl.getClusterInfo().getId(), dcTbl.getShardInfo().getId()))) {
					results.put(Triple.of(dcTbl.getId(), dcTbl.getClusterInfo().getId(), dcTbl.getShardInfo().getId()), dcTbl.getRedisInfo());
				}
			}
		}
		
		return results;
	}

	private HashMap<Long, DcTbl> loadAllDcs() {
		HashMap<Long, DcTbl> results = new HashMap<>();

		List<DcTbl> allDcs = dcService.findAllDcs();
		if(null != allDcs) {
			for(DcTbl dcTbl : allDcs) {
				results.put(dcTbl.getId(), dcTbl);
			}
		}

		return results;
	}

    private DcMetaQueryVO loadMetaVO(DcTbl currentDc, List<DcTbl> dcMetaDetails) {
    	logger.info("[CurrentDC]:" + coder.encode(currentDc));
    	DcMetaQueryVO result = new DcMetaQueryVO(currentDc);
    	
    	for(DcTbl dcMetaDetail : dcMetaDetails) {
    		logger.info("[LoadInfo]" + coder.encode(dcMetaDetail));
    		
	        /** Cluster Info **/
	        result.addClusterInfo(dcMetaDetail.getClusterInfo());
	        
	        /** Redis Info **/
	        result.addRedisInfo(dcMetaDetail.getRedisInfo());
	        
	        /** Shard Map **/
	        result.addShardMap(dcMetaDetail.getClusterInfo().getClusterName(), dcMetaDetail.getShardInfo());
	        	
	        /** Redis Detail **/
	        result.addRedisMap(dcMetaDetail.getClusterInfo().getClusterName(),
	        		dcMetaDetail.getShardInfo().getShardName(),
	        		dcMetaDetail.getRedisInfo());
	            
	        /** DcCluster Detail **/
	        result.addDcClusterMap(dcMetaDetail.getClusterInfo().getClusterName(), dcMetaDetail.getDcClusterInfo());
	            
	        /** DcClusterShard Detail **/
	        result.addDcClusterShardMap(dcMetaDetail.getClusterInfo().getClusterName(), 
	        		dcMetaDetail.getShardInfo().getShardName(), 
	        		dcMetaDetail.getDcClusterShardInfo());
	    }
    	
    	return result;
    }
}
