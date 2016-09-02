package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.Redis;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @author wenchao.meng
 *
 * Aug 18, 2016
 */
public class MetaUtils {
	
	public static boolean same(KeeperMeta keeperMeta1, KeeperMeta keeperMeta2){
		
		return theSame(keeperMeta1, keeperMeta2);
	}

	public static boolean same(RedisMeta redisMeta1, RedisMeta redisMeta2){

		return theSame(redisMeta1, redisMeta2);
		
	}

	private static boolean theSame(Redis redisMeta1, Redis redisMeta2) {
		
		if(!ObjectUtils.equals(redisMeta1.getIp(), redisMeta2.getIp())){
			return false;
		}

		if(!ObjectUtils.equals(redisMeta1.getPort(), redisMeta2.getPort())){
			return false;
		}
		
		return true;
	}
}
