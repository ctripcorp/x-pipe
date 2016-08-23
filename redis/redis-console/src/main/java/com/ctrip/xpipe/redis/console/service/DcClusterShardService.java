package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblDao;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblEntity;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.lookup.ContainerLoader;

import java.util.List;

import javax.annotation.PostConstruct;

@Service
public class DcClusterShardService {
    private DcClusterShardTblDao dcClusterShardTblDao;

    @PostConstruct
    private void postConstruct() {
        try {
            dcClusterShardTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterShardTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Dao construct failed.", e);
        }
    }

    public DcClusterShardTbl load(long dcClusterId, long shardId){
        try {
            return dcClusterShardTblDao.findDcClusterShard(shardId, dcClusterId, DcClusterShardTblEntity.READSET_FULL);
        } catch (DalNotFoundException e) {
            throw new DataNotFoundException("Dc-cluster-shard not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load dc-cluster-shard failed.", e);
        }
    }

    public DcClusterShardTbl load(String dcName, String clusterName, String shardName) {
        try {
            return dcClusterShardTblDao.findDcCluserShardByName(dcName, clusterName, shardName,
                    DcClusterShardTblEntity.READSET_FULL);
        } catch (DalNotFoundException e) {
            throw new DataNotFoundException("Dc-cluster-shard not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load dc-cluster-shard failed.", e);
        }
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
