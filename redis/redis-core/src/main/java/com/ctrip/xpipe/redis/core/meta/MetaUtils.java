package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.redis.core.entity.*;
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

	public static boolean same(ApplierMeta applierMeta1, ApplierMeta applierMeta2){

		return theSame(applierMeta1, applierMeta2);

	}

	public static boolean theSame(InstanceNode instanceNode1, InstanceNode instanceNode2) {
		
		if(instanceNode1 == null){
			return instanceNode2 == null;
		}else if(instanceNode2 == null){
			return false;
		}
		
		if(!ObjectUtils.equals( instanceNode1.getIp(), instanceNode2.getIp())){
			return false;
		}

		if(!ObjectUtils.equals( instanceNode1.getPort(), instanceNode2.getPort())){
			return false;
		}
		
		return true;
	}
}
