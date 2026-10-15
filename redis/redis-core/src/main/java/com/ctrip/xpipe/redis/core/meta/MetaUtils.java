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
		
		// 容器 Keeper 使用稳定的 hostname 作为身份，物理机 Keeper 继续使用 IP。
		if(instanceNode1 instanceof KeeperMeta && instanceNode2 instanceof KeeperMeta) {
			KeeperMeta keeper1 = (KeeperMeta) instanceNode1;
			KeeperMeta keeper2 = (KeeperMeta) instanceNode2;

			boolean hasHostname1 = !StringUtil.isEmpty(keeper1.getHostname());
			boolean hasHostname2 = !StringUtil.isEmpty(keeper2.getHostname());
			if (hasHostname1 != hasHostname2) {
				return false;
			}

			if (hasHostname1) {
				return ObjectUtils.equals(keeper1.getHostname(), keeper2.getHostname())
						&& ObjectUtils.equals(keeper1.getPort(), keeper2.getPort());
			}

			return ObjectUtils.equals(keeper1.getIp(), keeper2.getIp())
					&& ObjectUtils.equals(keeper1.getPort(), keeper2.getPort());
		}
		
		// 其他类型保持原有逻辑
		if(!ObjectUtils.equals( instanceNode1.getIp(), instanceNode2.getIp())){
			return false;
		}

		if(!ObjectUtils.equals( instanceNode1.getPort(), instanceNode2.getPort())){
			return false;
		}
		
		return true;
	}
}
