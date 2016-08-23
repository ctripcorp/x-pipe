package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblDao;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import java.util.List;

/**
 * @author shyin
 *
 * Aug 20, 2016
 */
@Service
public class DcClusterShardService extends AbstractConsoleService<DcClusterShardTblDao>{

    public DcClusterShardTbl load(final long dcClusterId, final long shardId){
    	return queryHandler.handleQuery(new DalQuery<DcClusterShardTbl>() {
			@Override
			public DcClusterShardTbl doQuery() throws DalNotFoundException, DalException {
				return dao.findDcClusterShard(shardId, dcClusterId, DcClusterShardTblEntity.READSET_FULL);
			}
    	});
    }

    public DcClusterShardTbl load(final String dcName, final String clusterName, final String shardName) {
    	return queryHandler.handleQuery(new DalQuery<DcClusterShardTbl>(){
			@Override
			public DcClusterShardTbl doQuery() throws DalNotFoundException, DalException {
				return dao.findDcCluserShardByName(dcName, clusterName, shardName, DcClusterShardTblEntity.READSET_FULL);
			}
    	});
    }
    
    @Transactional
    public void deleteDcClusterShards(String dcName, String clusterName, String shardName) {
    	/** TODO **/
    }
    
    @Transactional
    public void deleteDcClusterShardsBatch(List<DcClusterShardTbl> dcClusterShards) {
    	/** TODO **/
    }
    
}
