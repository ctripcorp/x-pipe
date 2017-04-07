package com.ctrip.xpipe.redis.console.dao;

import java.util.LinkedList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblDao;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblEntity;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.model.RedisTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;


/**
 * @author shyin
 *
 * Aug 29, 2016
 */
@Repository
public class DcClusterShardDao extends AbstractXpipeConsoleDAO{
	private DcClusterShardTblDao dcClusterShardTblDao;
	private RedisTblDao redisTblDao;
	
	@PostConstruct
	private void postConstruct() {
		try {
			dcClusterShardTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterShardTblDao.class);
			redisTblDao = ContainerLoader.getDefaultContainer().lookup(RedisTblDao.class);
		} catch (ComponentLookupException e) {
			throw new ServerException("Cannot construct dao.", e);
		}
	}
	
	@DalTransaction
	public void deleteDcClusterShardsBatch(List<DcClusterShardTbl> dcClusterShards) throws DalException {
		if(null == dcClusterShards) throw new DalException("Null cannot be deleted.");
		
		List<RedisTbl> redises = new LinkedList<RedisTbl>();
		for(final DcClusterShardTbl dcClusterShard : dcClusterShards) {
			List<RedisTbl> relatedRedises = queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
				@Override
				public List<RedisTbl> doQuery() throws DalException {
					return redisTblDao.findAllByDcClusterShardId(dcClusterShard.getDcClusterShardId(), null, RedisTblEntity.READSET_FULL);
				}
			});
			
			if(null != relatedRedises) {
				for(RedisTbl redis : relatedRedises) {
					redis.setRunId(generateDeletedName(redis.getRunId()));
				}
				redises.addAll(relatedRedises);
			}
		}
		redisTblDao.deleteBatch(redises.toArray(new RedisTbl[redises.size()]), RedisTblEntity.UPDATESET_FULL);
		
		dcClusterShardTblDao.deleteDcClusterShardsBatch(dcClusterShards.toArray(new DcClusterShardTbl[dcClusterShards.size()]), DcClusterShardTblEntity.UPDATESET_FULL);
	}

}
