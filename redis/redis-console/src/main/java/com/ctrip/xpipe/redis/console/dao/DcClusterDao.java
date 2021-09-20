package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.LinkedList;
import java.util.List;


/**
 * @author shyin
 *
 * Aug 29, 2016
 */
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
	
	@DalTransaction
	public void deleteDcClustersBatch(List<DcClusterTbl> dcClusters) throws DalException {
		if(null == dcClusters || dcClusters.isEmpty()) {
			logger.warn("[deleteDcClustersBatch] Empty dcClusters list: {}", dcClusters);
			return;
		}
		
		List<DcClusterShardTbl> dcClusterShards = new LinkedList<DcClusterShardTbl>();
		for(final DcClusterTbl dcCluster : dcClusters) {
			List<DcClusterShardTbl> relatedDcClusterShards = queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>(){
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

		queryHandler.handleBatchDelete(new DalQuery<int[]>() {
			@Override
			public int[] doQuery() throws DalException {
				return dcClusterTblDao.deleteBatch(dcClusters.toArray(new DcClusterTbl[dcClusters.size()]),
						DcClusterTblEntity.UPDATESET_FULL);
			}
		}, true);

	}

	@DalTransaction
	public void deleteDcClustersBatch(final DcClusterTbl dcCluster) throws DalException  {
		if(null == dcCluster) throw new DalException("Null cannot be deleted.");
		
		List<DcClusterShardTbl> dcClusterShards = queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>(){
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dcClusterShardTblDao.findAllByDcClusterId(dcCluster.getDcClusterId(), DcClusterShardTblEntity.READSET_FULL);
			}
		});
		
		if(null != dcClusterShards) {
			dcClusterShardDao.deleteDcClusterShardsBatch(dcClusterShards);
		}

		queryHandler.handleDelete(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dcClusterTblDao.deleteBatch(dcCluster, DcClusterTblEntity.UPDATESET_FULL);
			}
		}, true);

	}
	
}
