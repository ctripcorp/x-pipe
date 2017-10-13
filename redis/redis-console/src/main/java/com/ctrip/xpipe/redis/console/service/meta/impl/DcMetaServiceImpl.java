package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.KeepercontainerService;
import com.ctrip.xpipe.redis.console.service.SentinelService;
import com.ctrip.xpipe.redis.console.service.meta.*;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaQueryVO;
import com.ctrip.xpipe.redis.console.util.DataModifiedTimeGenerator;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
@Service
public class DcMetaServiceImpl extends AbstractMetaService implements DcMetaService {
	@Autowired
	private DcService dcService;
	@Autowired
	private DcClusterService dcClusterService;
	@Autowired
	private SentinelService sentinelService;
	@Autowired
	private KeepercontainerService keepercontainerService;
	@Autowired 
	private SentinelMetaService setinelMetaService;
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
				return dcService.find(dcName);
			}
    	});
    	Future<List<SetinelTbl>> future_sentinels = fixedThreadPool.submit(new Callable<List<SetinelTbl>>() {
			@Override
			public List<SetinelTbl> call() throws DalException {
				return sentinelService.findAllByDcName(dcName);
			}
    	});
    	Future<List<KeepercontainerTbl>> future_keepercontainers = fixedThreadPool.submit(new Callable<List<KeepercontainerTbl>>() {
			@Override
			public List<KeepercontainerTbl> call() throws DalException {
				return keepercontainerService.findAllByDcName(dcName);
			}
    	});
    	Future<HashMap<Long, DcTbl>> future_alldcs = fixedThreadPool.submit(new Callable<HashMap<Long, DcTbl>>() {
			@Override
			public HashMap<Long, DcTbl> call() throws DalException {
				return loadAllDcs();
			}
    	});
    	Future<HashMap<Long, List<DcClusterTbl>>> future_allDcClusters = fixedThreadPool.submit(new Callable<HashMap<Long, List<DcClusterTbl>>>() {
			@Override
			public HashMap<Long, List<DcClusterTbl>> call() throws Exception {
				return loadAllDcClusters();
			}
		});
    	Future<List<DcTbl>> future_allDetails = fixedThreadPool.submit(new Callable<List<DcTbl>>() {
			@Override
			public List<DcTbl> call() throws Exception {
				return dcService.findAllDetails(dcName);
			}
		});
    	
		try {
			DcTbl dcInfo = future_dcInfo.get();
			if(null == dcInfo) return dcMeta;
			
			dcMeta.setId(dcInfo.getDcName());
	    	dcMeta.setLastModifiedTime(dcInfo.getDcLastModifiedTime());

	    	if(null != future_sentinels.get()) {
	    		for(SetinelTbl setinel : future_sentinels.get()) {
	    			dcMeta.addSentinel(setinelMetaService.encodeSetinelMeta(setinel, dcMeta));
		    	}
	    	}
	    	if(null != future_keepercontainers.get()) {
	    		for(KeepercontainerTbl keepercontainer : future_keepercontainers.get()) {
		    		dcMeta.addKeeperContainer(keepercontainerMetaService.encodeKeepercontainerMeta(keepercontainer, dcMeta));
		    	}
	    	}
	    	
	    	List<DcTbl> allDetails = future_allDetails.get();
	    	if(null != allDetails) {
	    		DcMetaQueryVO dcMetaQueryVO = loadMetaVO(dcInfo, allDetails);
	    		
				if(null != future_alldcs.get()) {
					dcMetaQueryVO.setAllDcs(future_alldcs.get());
				}
				
				if(null != future_allDcClusters.get()) {
					dcMetaQueryVO.setAllDcClusterMap(future_allDcClusters.get());
				}
				
				for(ClusterTbl cluster : dcMetaQueryVO.getClusterInfo().values()){
					dcMeta.addCluster(clusterMetaService.loadClusterMeta(dcMeta, cluster, dcMetaQueryVO));
				}
	    	}
			
		} catch (ExecutionException e) {
			throw new ServerException("Execution failed.", e);
		} catch (InterruptedException e) {
			throw new ServerException("Concurrent execution failed.", e);
		} finally {
			fixedThreadPool.shutdown();
		}
    	return dcMeta;
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
	
	private HashMap<Long, List<DcClusterTbl>> loadAllDcClusters() {
		HashMap<Long, List<DcClusterTbl>> results = new HashMap<>();
		List<DcClusterTbl> allDcClusters = dcClusterService.findAllDcClusters();
		if(null != allDcClusters) {
			for(DcClusterTbl dcClusterTbl : allDcClusters) {
				if(null == results.get(dcClusterTbl.getClusterId())) {
					LinkedList<DcClusterTbl> dcClusterTblList = new LinkedList<>();
					dcClusterTblList.add(dcClusterTbl);
					results.put(dcClusterTbl.getClusterId(), dcClusterTblList);
				} else {
					results.get(dcClusterTbl.getClusterId()).add(dcClusterTbl);
				}
			}
		}
		return results;
	}

    private DcMetaQueryVO loadMetaVO(DcTbl currentDc, List<DcTbl> dcMetaDetails) {
    	DcMetaQueryVO result = new DcMetaQueryVO(currentDc);
    	
    	if(null != dcMetaDetails) {
    		for(DcTbl dcMetaDetail : dcMetaDetails) {
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
    	}
    	
    	return result;
    }
}
