package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.Redis;

/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public class RedisComparator extends AbstractMetaComparator<Redis, Object, RedisChange>{
	
	public RedisComparator(Redis current, Redis future) {
		super(current, future);
	}

	@Override
	public void compare() {
		//too many redis meta, avoid reflection
	}

	@Override
	public boolean isShallowChange() {
		throw new UnsupportedOperationException("too many redis meta, avoid reflection. realize it yourself if you need it indeed. ");
	}

	@Override
	public String idDesc() {
		return current.desc();
	}

	public Redis getCurrent() {
		return current;
	}

	public Redis getFuture() {
		return future;
	}
}
