package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.Redis;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.List;

/**
 * @author wenchao.meng
 *
 * Aug 18, 2016
 */
public class MetaUtils {

	public static String toString(List<? extends Redis> redises){

		return StringUtil.join(",", (redis) -> redis.desc(), redises);
	}
	
	public static boolean same(KeeperMeta keeperMeta1, KeeperMeta keeperMeta2){
		
		return theSame(keeperMeta1, keeperMeta2);
	}

	public static boolean same(RedisMeta redisMeta1, RedisMeta redisMeta2){

		return theSame(redisMeta1, redisMeta2);
		
	}

	public static boolean theSame(Redis redisMeta1, Redis redisMeta2) {
		
		if(redisMeta1== null){
			return redisMeta2 == null;
		}else if(redisMeta2 == null){
			return false;
		}
		
		if(!ObjectUtils.equals(redisMeta1.getIp(), redisMeta2.getIp())){
			return false;
		}

		if(!ObjectUtils.equals(redisMeta1.getPort(), redisMeta2.getPort())){
			return false;
		}
		
		return true;
	}
}
