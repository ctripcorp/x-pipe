package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import jakarta.annotation.PostConstruct;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import java.util.LinkedList;
import java.util.List;


/**
 * @author shyin
 *
 * Aug 29, 2016
 */
@Repository
public class DcClusterShardDao extends AbstractXpipeConsoleDAO{

	private DcClusterShardTblDao dcClusterShardTblDao;

	@Autowired
	private ApplierDao applierDao;

	@Autowired
	private RedisDao redisDao;
	
	@PostConstruct
	private void postConstruct() {
		try {
			dcClusterShardTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterShardTblDao.class);
		} catch (ComponentLookupException e) {
			throw new ServerException("Cannot construct dao.", e);
		}
	}
	
	@DalTransaction
	public void deleteDcClusterShardsBatch(List<DcClusterShardTbl> dcClusterShards) throws DalException {
		if(null == dcClusterShards || dcClusterShards.isEmpty()) {
			logger.warn("[deleteDcClusterShardsBatch] Empty list: {}", dcClusterShards);
			return;
		}
		
		List<RedisTbl> redises = new LinkedList<RedisTbl>();
		List<ApplierTbl> appliers = new LinkedList<ApplierTbl>();
		for(final DcClusterShardTbl dcClusterShard : dcClusterShards) {
			List<RedisTbl> relatedRedises = redisDao.findAllByDcClusterShard(dcClusterShard.getDcClusterShardId());
			if(null != relatedRedises && !relatedRedises.isEmpty()) {
				for(RedisTbl redis : relatedRedises) {
					redis.setRunId(generateDeletedName(redis.getRunId()));
				}
				redises.addAll(relatedRedises);
			}

			List<ApplierTbl> relatedAppliers = applierDao.findByDcClusterShard(dcClusterShard.getDcClusterShardId());
			if(null != relatedAppliers && !relatedAppliers.isEmpty()) {
				appliers.addAll(relatedAppliers);
			}
		}
		redisDao.deleteRedisesBatch(redises);
		applierDao.deleteApplierBatch(appliers);

		queryHandler.handleBatchDelete(new DalQuery<int[]>() {
			@Override
			public int[] doQuery() throws DalException {
				return dcClusterShardTblDao.deleteDcClusterShardsBatch(
						dcClusterShards.toArray(new DcClusterShardTbl[dcClusterShards.size()]),
						DcClusterShardTblEntity.UPDATESET_FULL);
			}
		}, true);
	}
}
