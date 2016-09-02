package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.dao.RedisDao;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.model.RedisTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.meta.ShardMetaService;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.Comparator;
import java.util.List;

/**
 * @author shyin
 * 
 * Aug 20, 2016
 */
@Service
public class RedisService extends AbstractConsoleService<RedisTblDao>{
	public static int IS_MASTER = Integer.MAX_VALUE;
	public static int MASTER_REQUIRED = Integer.MIN_VALUE;
	public static int NO_EXIST_ID = 0;
	
	@Autowired
	ShardMetaService shardMetaService;
	@Autowired
	DcClusterShardService dcClusterShardService;
	@Autowired
	RedisDao redisDao;

    public List<RedisTbl> findByDcClusterShardId(final long dcClusterShardId){
    	return queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
			@Override
			public List<RedisTbl> doQuery() throws DalException {
				return dao.findAllByDcClusterShardId(dcClusterShardId, RedisTblEntity.READSET_FULL);
			}
    	});
    }
    
    public RedisTbl load(final long id) {
    	return queryHandler.handleQuery(new DalQuery<RedisTbl>() {
			@Override
			public RedisTbl doQuery() throws DalException {
				return dao.findByPK(id, RedisTblEntity.READSET_FULL);
			}
    	});
    }
    
    public void updateRedises(String clusterName, String dcName, String shardName, ShardMeta targetShardMeta) {
    	if(null == targetShardMeta) throw new BadRequestException("RequestBody cannot be null.");
    	final DcClusterShardTbl dcClusterShard = dcClusterShardService.load(dcName, clusterName, shardName);
    	if(null == dcClusterShard) throw new BadRequestException("Cannot find dc-cluster-shard.");
    	
    	ShardMeta originShardMeta = shardMetaService.getShardMeta(dcName, clusterName, shardName);
    	
    	getToDelete(originShardMeta, targetShardMeta);
    }

    public ShardMeta getToDelete(ShardMeta origin, ShardMeta target) {
    	Comparator<KeeperMeta> keeperMetaComparator = new Comparator<KeeperMeta>() {
			@Override
			public int compare(KeeperMeta o1, KeeperMeta o2) {
				if(o1.getId().equals(o2.getId()) && o1.getIp().equals(o2.getIp()) && (o1.getKeeperContainerId() == o2.getKeeperContainerId())
						&& (o1.getPort() == o2.getPort())) {
					return 0;
				}
				return -1;
			}
    	};
    	List<KeeperMeta> originKeeper = origin.getKeepers();
    	List<KeeperMeta> targetKeeper = target.getKeepers();
    	
    	List<KeeperMeta> toDeleteKeeeper = (List<KeeperMeta>) setOperator.difference(KeeperMeta.class, originKeeper, targetKeeper, keeperMetaComparator);
    	return null;
    }
    
    public RedisTbl findActiveKeeper(List<RedisTbl> redises) {
    	if(null == redises) return null;
    	
    	RedisTbl result = null;
    	for(RedisTbl redis : redises) {
    		if(redis.getRedisRole().equals("keeper") && (redis.isKeeperActive() == true )) {
    			result = redis;
    			break;
    		}
    	}
    	return result;
    }

}
