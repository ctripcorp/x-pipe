package com.ctrip.xpipe.redis.console.dao;

import java.util.LinkedList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblDao;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblEntity;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTblDao;
import com.ctrip.xpipe.redis.console.model.DcClusterTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;

@Repository
public class DcClusterDao extends AbstractXpipeConsoleDAO{
	private DcClusterTblDao dcClusterTblDao;
	private DcClusterShardTblDao dcClusterShardTblDao;
	
	@Autowired
	private DcClusterShardDao dcClusterShardDao;
	
	@PostConstruct
	private void postConstruct() {
		try {
			dcClusterTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterTblDao.class);
			dcClusterShardTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterShardTblDao.class);
		} catch (ComponentLookupException e) {
			throw new ServerException("Cannot construct dao.", e);
		}
	}
	
	public void deleteDcClustersBatch(List<DcClusterTbl> dcClusters) throws DalException {
		if(null == dcClusters) return;
		
		List<DcClusterShardTbl> dcClusterShards = new LinkedList<DcClusterShardTbl>();
		for(final DcClusterTbl dcCluster : dcClusters) {
			List<DcClusterShardTbl> relatedDcClusterShards = queryHandler.tryGet(new DalQuery<List<DcClusterShardTbl>>(){
				@Override
				public List<DcClusterShardTbl> doQuery() throws DalException {
					return dcClusterShardTblDao.findAllByDcClusterId(dcCluster.getDcClusterId(), DcClusterShardTblEntity.READSET_FULL);
				}
			});
			
			if(null != relatedDcClusterShards) {
				dcClusterShards.addAll(relatedDcClusterShards);
			}
		}
		dcClusterShardDao.deleteDcClusterShardsBatch(dcClusterShards);
		
		dcClusterTblDao.deleteBatch(dcClusters.toArray(new DcClusterTbl[dcClusters.size()]), DcClusterTblEntity.UPDATESET_FULL);
	}
	
	
	
	public int bindDc(String clusterName, String dcName) {
		// TODO
		return 0;
	}
	
	public int unbindDc(String clusterName, String dcName) {
		// TODO
		return 0;
	}
	
}
