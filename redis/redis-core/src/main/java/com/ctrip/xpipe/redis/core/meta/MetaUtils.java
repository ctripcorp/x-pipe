package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @author wenchao.meng
 *
 * Aug 18, 2016
 */
public class MetaUtils {
	
	public static boolean same(KeeperMeta keeperMeta1, KeeperMeta keeperMeta2){
		
		if(!ObjectUtils.equals(keeperMeta1.getIp(), keeperMeta2.getIp())){
			return false;
		}

		if(!ObjectUtils.equals(keeperMeta1.getPort(), keeperMeta2.getPort())){
			return false;
		}
		
		return true;
	}

}
