package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.*;
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
public class ShardService extends AbstractConsoleService<ShardTblDao>{

    public ShardTbl load(final long shardId){
    	return queryHandler.handleQuery(new DalQuery<ShardTbl>() {
			@Override
			public ShardTbl doQuery() throws DalNotFoundException, DalException {
				return dao.findByPK(shardId, ShardTblEntity.READSET_FULL);
			}
    	});
    }
    
    public ShardTbl load(final String clusterName, final String shardName) {
    	return queryHandler.handleQuery(new DalQuery<ShardTbl>() {
			@Override
			public ShardTbl doQuery() throws DalNotFoundException, DalException {
				return dao.findShard(clusterName, shardName, ShardTblEntity.READSET_FULL);
			}
    	});
    }

    public List<ShardTbl> loadAllByClusterName(final String clusterName){
    	return queryHandler.handleQuery(new DalQuery<List<ShardTbl>>() {
			@Override
			public List<ShardTbl> doQuery() throws DalNotFoundException, DalException {
				return dao.findAllByClusterName(clusterName, ShardTblEntity.READSET_FULL);
			}
    	});
    }
    
    public List<ShardTbl> findAllShardNamesByClusterName(final String clusterName) {
    	return queryHandler.handleQuery(new DalQuery<List<ShardTbl>>() {
			@Override
			public List<ShardTbl> doQuery() throws DalNotFoundException, DalException {
				return dao.findAllByClusterName(clusterName, ShardTblEntity.READSET_NAME);
			}
    	});
    }
    
    @Transactional
    public void deleteShards(String clusterName, String shardName) {
    	// TODO
    	/** Delete shard-info together with dc-clsuter-shard && redises under d-c-s **/
    }
    
    @Transactional
    public void deleteShardsBatch(List<ShardTbl> shards) {
    	// TODO
    }
    
    
}
