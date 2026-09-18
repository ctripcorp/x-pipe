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
		
		// 对于KeeperMeta: IP有值优先用IP比对,IP为空或0.0.0.0时用hostname比对
		if(instanceNode1 instanceof KeeperMeta && instanceNode2 instanceof KeeperMeta) {
			KeeperMeta keeper1 = (KeeperMeta) instanceNode1;
			KeeperMeta keeper2 = (KeeperMeta) instanceNode2;
			
			String ip1 = keeper1.getIp();
			String ip2 = keeper2.getIp();
			
			// 判断是否为有效的IP地址(非空且非0.0.0.0)
			boolean hasValidIp1 = !StringUtil.isEmpty(ip1) && !"0.0.0.0".equals(ip1);
			boolean hasValidIp2 = !StringUtil.isEmpty(ip2) && !"0.0.0.0".equals(ip2);
			
			// 如果两个都有有效IP,按IP比对
			if(hasValidIp1 && hasValidIp2) {
				if(!ObjectUtils.equals(ip1, ip2)){
					return false;
				}
				if(!ObjectUtils.equals(instanceNode1.getPort(), instanceNode2.getPort())){
					return false;
				}
				return true;
			}
			
			// 如果两个都没有有效IP,用hostname比对
			if(!hasValidIp1 && !hasValidIp2) {
				if(!ObjectUtils.equals(keeper1.getHostname(), keeper2.getHostname())){
					return false;
				}
				if(!ObjectUtils.equals(instanceNode1.getPort(), instanceNode2.getPort())){
					return false;
				}
				return true;
			}
			
			// 一个有IP一个没有,不相等
			return false;
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
