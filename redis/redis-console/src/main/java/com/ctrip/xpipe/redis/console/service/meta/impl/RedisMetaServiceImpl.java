package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.dao.RedisDao;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.redis.console.service.meta.AbstractMetaService;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(false)
public class RedisMetaServiceImpl extends AbstractMetaService implements RedisMetaService {
	public static long REDIS_MASTER_NULL = 0L;
	
	@Autowired
	private RedisService redisService;

	@Override
	public RedisMeta getRedisMeta(ShardMeta shardMeta, RedisTbl redisInfo) {
		RedisMeta redisMeta = new RedisMeta();
		
		if(null != redisInfo) {
			redisMeta.setId(redisInfo.getRunId());
			redisMeta.setIp(redisInfo.getRedisIp());
			redisMeta.setPort(redisInfo.getRedisPort());
			if(redisInfo.isMaster()) {
				redisMeta.setMaster("");
			} else {
				redisMeta.setMaster(XPipeConsoleConstant.DEFAULT_ADDRESS);
			}
		}
		
		redisMeta.setParent(shardMeta);
		return redisMeta;
	}

	@Override
	public KeeperMeta getKeeperMeta(ShardMeta shardMeta, RedisTbl redisInfo) {
		KeeperMeta keeperMeta = new KeeperMeta();
		
		if(null != redisInfo) {
			keeperMeta.setId(redisInfo.getRunId());
			keeperMeta.setIp(redisInfo.getRedisIp());
			keeperMeta.setPort(redisInfo.getRedisPort());
			keeperMeta.setActive(redisInfo.isKeeperActive());
			keeperMeta.setKeeperContainerId(redisInfo.getKeepercontainerId());
		}
		
		keeperMeta.setParent(shardMeta);
		
		return keeperMeta;
	}

	@Override
	public void updateKeeperStatus(String dcId, String clusterId, String shardId, KeeperMeta newActiveKeeper) throws ResourceNotFoundException {

		List<RedisTbl> keepers = RedisDao.findWithRole(redisService.findAllByDcClusterShard(dcId, clusterId, shardId), XPipeConsoleConstant.ROLE_KEEPER);
		if (CollectionUtils.isEmpty(keepers)){
			return;
		}

		for(RedisTbl keeper: keepers) {
			if (keeper.getKeepercontainerId() == newActiveKeeper.getKeeperContainerId()){
				keeper.setKeeperActive(true);
			} else {
				keeper.setKeeperActive(false);
			}
		}
		redisService.updateBatchKeeperActive(keepers);
	}

}
