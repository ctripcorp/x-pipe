package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.List;

@Service
public class ShardService {
    private ShardTblDao shardTblDao;

    @PostConstruct
    private void postConstruct() {
        try {
            shardTblDao = ContainerLoader.getDefaultContainer().lookup(ShardTblDao.class);
        } catch (Exception e) {
            throw new ServerException("Dao construct failed.", e);
        }
    }

    public ShardTbl load(long shardId){
        try {
            return shardTblDao.findByPK(shardId, ShardTblEntity.READSET_FULL);
        } catch (DalNotFoundException e) {
            throw new DataNotFoundException("Shard not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load shard failed.", e);
        }
    }
    
    public ShardTbl load(String clusterName, String shardName) {
        try {
            return shardTblDao.findShard(clusterName, shardName, ShardTblEntity.READSET_FULL);
        } catch (DalNotFoundException e) {
            throw new DataNotFoundException("Shard not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load shard failed.", e);
        }
    }

    public List<ShardTbl> loadAllByClusterName(String clusterName){
        try {
            return shardTblDao.findAllByClusterName(clusterName, ShardTblEntity.READSET_FULL);
        } catch (DalNotFoundException e) {
            throw new DataNotFoundException("Shards not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load all shards by cluster-name failed.", e);
        }
    }
    
    @Transactional
    public void deleteShards(String clusterName, String shardName) {
    	/** TODO **/
    	/** Delete shard-info together with dc-clsuter-shard && redises under d-c-s **/
    }
    
    @Transactional
    public void deleteShardsBatch(List<ShardTbl> shards) {
    	/** TODO **/
    }
    
    
}
