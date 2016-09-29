package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.constant.XpipeConsoleConstant;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblDao;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblEntity;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.model.RedisTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author shyin
 *
 * Aug 31, 2016
 */
@Repository
public class RedisDao  extends AbstractXpipeConsoleDAO{
	private RunidGenerator idGenerator = RunidGenerator.DEFAULT;
	private RedisTblDao redisTblDao;
	private DcClusterShardTblDao dcClusterShardTblDao;
	
	@PostConstruct
	private void postConstruct() {
		try {
			redisTblDao = ContainerLoader.getDefaultContainer().lookup(RedisTblDao.class);
			dcClusterShardTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterShardTblDao.class);
		} catch (ComponentLookupException e) {
			throw new ServerException("Cannot construct dao.", e);
		}
	}
	
	@DalTransaction
	public void createRedisesBatch(List<RedisTbl> redises) throws DalException {
		if(null != redises) {
			Map<Long,String> cache = new HashMap<Long,String>();
			for(RedisTbl redis : redises) {
				if(redis.getRedisRole().equals(XpipeConsoleConstant.ROLE_KEEPER)) {
					if(null == cache.get(redis.getDcClusterShardId())) {
						String newKeeperId = getToCreateKeeperId(redis);
						redis.setRunId(newKeeperId);
						cache.put(redis.getDcClusterShardId(), newKeeperId);
					} else {
						redis.setRunId(cache.get(redis.getDcClusterShardId()));
					}
				}
			}
			redisTblDao.insertBatch(redises.toArray(new RedisTbl[redises.size()]));
		}
	}
	
	@DalTransaction
	public RedisTbl createRedisesBatch(RedisTbl redis) throws DalException {
		if(null == redis) throw new BadRequestException("Null redis cannot be created");
		
		if(redis.getRedisRole().equals(XpipeConsoleConstant.ROLE_KEEPER)) {
			redis.setRunId(getToCreateKeeperId(redis));
		}
		redisTblDao.insertBatch(redis);
		return redisTblDao.findWithBasicConfigurations(redis.getRunId(), redis.getDcClusterShardId(), redis.getRedisIp(), redis.getRedisPort(), RedisTblEntity.READSET_FULL);
	}
	
	@DalTransaction
	public void deleteRedisesBatch(List<RedisTbl> redises) throws DalException {
		if(null != redises) {
			for(RedisTbl redis : redises) {
				redis.setRunId(generateDeletedName(redis.getRunId()));
			}
		}
		redisTblDao.deleteBatch(redises.toArray(new RedisTbl[redises.size()]), RedisTblEntity.UPDATESET_FULL);
	}

	@DalTransaction
	public void updateBatch(List<RedisTbl> redises) throws DalException {
		redisTblDao.updateBatch(redises.toArray(new RedisTbl[redises.size()]), RedisTblEntity.UPDATESET_FULL);
	}
	
	@DalTransaction
	public void handleUpdate(List<RedisTbl> toCreate, List<RedisTbl> toDelete, List<RedisTbl> left) throws DalException {
		if(null != toCreate && toCreate.size() > 0) {
    		createRedisesBatch(toCreate);
    	}

		if(null != toDelete && toDelete.size() > 0) {
    		deleteRedisesBatch(toDelete);
    	}
		
    	if(null != left && left.size() > 0) {
    		updateBatch(left);
    	}
	}

	private String getToCreateKeeperId(final RedisTbl redis) {
		if(null == redis) throw new BadRequestException("Cannot obtain keeper-id from null.");
		List<RedisTbl> dcClusterShardRedises = queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
			@Override
			public List<RedisTbl> doQuery() throws DalException {
				return redisTblDao.findAllWithHistoryByDcClusterShardId(redis.getDcClusterShardId(), RedisTblEntity.READSET_FULL);
			}
		});
		
		if(null == dcClusterShardRedises) {
			return generateUniqueKeeperId(redis);
		} else {
			List<RedisTbl> keepers = RedisService.findWithRole(dcClusterShardRedises, XpipeConsoleConstant.ROLE_KEEPER);
			if(null != keepers && keepers.size() > 0) {
				RedisTbl historyKeeper = keepers.get(0);
				if(historyKeeper.isDeleted() == true) {
					int index = historyKeeper.getRunId().indexOf(DELETED_NAME_SPLIT_TAG);
					if(index > 0) {
						return historyKeeper.getRunId().substring(index + 1);
					}
					return historyKeeper.getRunId();
				} else {
					return historyKeeper.getRunId();
				}
			} else {
				return generateUniqueKeeperId(redis);
			}
		}
	}
	
	private String generateUniqueKeeperId(final RedisTbl redis) {
		final String runId = idGenerator.generateRunid();
		
		// check for unique runId
		DcClusterShardTbl targetDcClusterShard = queryHandler.handleQuery(new DalQuery<DcClusterShardTbl>() {
			@Override
			public DcClusterShardTbl doQuery() throws DalException {
				return dcClusterShardTblDao.findByPK(redis.getDcClusterShardId(), DcClusterShardTblEntity.READSET_FULL);
			}
		});
		if(null == targetDcClusterShard) throw new BadRequestException("Cannot find related dc-cluster-shard");
		
		List<RedisTbl> redisWithSameRunId = queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
			@Override
			public List<RedisTbl> doQuery() throws DalException {
				return redisTblDao.findByRunid(runId, RedisTblEntity.READSET_FULL);
			}
		});
	
		if(null != redisWithSameRunId && redisWithSameRunId.size() > 0) {
			for(final RedisTbl tmpRedis : redisWithSameRunId) {
				DcClusterShardTbl tmpDcClusterShard = queryHandler.handleQuery(new DalQuery<DcClusterShardTbl>() {
					@Override
					public DcClusterShardTbl doQuery() throws DalException {
						return dcClusterShardTblDao.findByPK(tmpRedis.getDcClusterShardId(), DcClusterShardTblEntity.READSET_FULL);
					}
				});
				if(null != tmpDcClusterShard && targetDcClusterShard != tmpDcClusterShard 
						&& targetDcClusterShard.getShardId() == tmpDcClusterShard.getShardId()) {
					throw new ServerException("Cannot generate unque keeper id, please retry.");
				}
			}
		}
		
		return runId;
	}
}
