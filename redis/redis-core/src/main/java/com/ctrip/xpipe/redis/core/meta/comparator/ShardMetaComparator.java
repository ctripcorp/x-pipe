package com.ctrip.xpipe.redis.core.meta.comparator;


import com.ctrip.xpipe.redis.core.entity.Redis;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public class ShardMetaComparator extends AbstractMetaComparator<Redis>{
	
	private ShardMeta current, future;
	
	public ShardMetaComparator(ShardMeta current, ShardMeta future){
		this.current = current;
		this.future = future;
	}

	@Override
	public void compare() {
		current.getRedises();
		
		
	}

}
