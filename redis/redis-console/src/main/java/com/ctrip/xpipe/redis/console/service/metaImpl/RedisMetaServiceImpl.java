package com.ctrip.xpipe.redis.console.service.metaImpl;

import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaQueryVO;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
@Service("redisMetaService")
public class RedisMetaServiceImpl implements RedisMetaService{
	public static long REDIS_MASTER_NULL = 0L;
	
	@Autowired
	RedisService redisService;
	
	@Override
	public String encodeRedisAddress(RedisTbl redisTbl) {
		if(null == redisTbl) {
			return "";
		} else {
			StringBuilder sb = new StringBuilder(30);
			sb.append(redisTbl.getRedisIp());
			sb.append(":");
			sb.append(String.valueOf(redisTbl.getRedisPort()));
			return sb.toString();
		}
	}
	
	@Override
	public RedisMeta loadRedisMeta(ShardMeta shardMeta, RedisTbl redisTbl, DcMetaQueryVO dcMetaQueryVO) {
		RedisMeta redisMeta = new RedisMeta();
		
		redisMeta.setId(redisTbl.getRedisName());
		redisMeta.setIp(redisTbl.getRedisIp());
		redisMeta.setPort(redisTbl.getRedisPort());
		if(redisTbl.getRedisMaster() == REDIS_MASTER_NULL) {
			redisMeta.setMaster("");
		} else {
			if(dcMetaQueryVO.getRedisInfo().containsKey(redisTbl.getRedisMaster())) {
				redisMeta.setMaster(encodeRedisAddress(dcMetaQueryVO.getRedisInfo().get(redisTbl.getRedisMaster())));
			} else {
				for(RedisTbl redis : dcMetaQueryVO.getAllActiveKeepers().values()) {
					if (redis.getId() == redisTbl.getRedisMaster()) {
						redisMeta.setMaster(encodeRedisAddress(redis));
						break;
					}
				}
			}
		}
		redisMeta.setParent(shardMeta);
		return redisMeta;
	}

	@Override
	public KeeperMeta loadKeeperMeta(ShardMeta shardMeta, RedisTbl redisTbl, DcMetaQueryVO dcMetaQueryVO) {
		KeeperMeta keeperMeta = new KeeperMeta();
		
		keeperMeta.setId(redisTbl.getRedisName());
		keeperMeta.setIp(redisTbl.getRedisIp());
		keeperMeta.setPort(redisTbl.getRedisPort());
		if(redisTbl.getRedisMaster() == REDIS_MASTER_NULL) {
			keeperMeta.setMaster("");
		} else {
			if(dcMetaQueryVO.getRedisInfo().containsKey(redisTbl.getRedisMaster())) {
				keeperMeta.setMaster(encodeRedisAddress(dcMetaQueryVO.getRedisInfo().get(redisTbl.getRedisMaster())));
			} else {
				for(RedisTbl redis : dcMetaQueryVO.getAllActiveKeepers().values()) {
					if(redis.getId() == redisTbl.getRedisMaster()) {
						keeperMeta.setMaster(encodeRedisAddress(redis));
						break;
					}
				}
			}
		}
		keeperMeta.setActive(redisTbl.isKeeperActive());
		keeperMeta.setKeeperContainerId(redisTbl.getKeepercontainerId());
		keeperMeta.setParent(shardMeta);
		return keeperMeta;
	}

	@Override
	public RedisMeta getRedisMeta(ShardMeta shardMeta, RedisTbl redisInfo, Map<Long,RedisTbl> redises) {
		RedisMeta redisMeta = new RedisMeta();
		
		redisMeta.setId(redisInfo.getRedisName());
		redisMeta.setIp(redisInfo.getRedisIp());
		redisMeta.setPort(redisInfo.getRedisPort());
		if(redisInfo.getRedisMaster() == REDIS_MASTER_NULL) {
			redisMeta.setMaster("");
		} else {
			if(null != redises.get(redisInfo.getRedisMaster())) {
				redisMeta.setMaster(encodeRedisAddress(redises.get(redisInfo.getRedisMaster())));
			} else {
				redisMeta.setMaster(encodeRedisAddress(redisService.load(redisInfo.getRedisMaster())));
			}
		}
		redisMeta.setParent(shardMeta);
				
		return redisMeta;
	}

	@Override
	public KeeperMeta getKeeperMeta(ShardMeta shardMeta, RedisTbl redisInfo, Map<Long,RedisTbl> redises) {
		KeeperMeta keeperMeta = new KeeperMeta();
		
		keeperMeta.setId(redisInfo.getRedisName());
		keeperMeta.setIp(redisInfo.getRedisIp());
		keeperMeta.setPort(redisInfo.getRedisPort());
		if(redisInfo.getRedisMaster() == REDIS_MASTER_NULL) {
			keeperMeta.setMaster("");
		} else {
			if(null != redises.get(redisInfo.getRedisMaster())) {
				keeperMeta.setMaster(encodeRedisAddress(redises.get(redisInfo.getRedisMaster())));
			} else {
				keeperMeta.setMaster(encodeRedisAddress(redisService.load(redisInfo.getRedisMaster())));
			}
		}
		keeperMeta.setActive(redisInfo.isKeeperActive());
		keeperMeta.setKeeperContainerId(redisInfo.getKeepercontainerId());
		keeperMeta.setParent(shardMeta);
		
		return keeperMeta;
	}


	
}
