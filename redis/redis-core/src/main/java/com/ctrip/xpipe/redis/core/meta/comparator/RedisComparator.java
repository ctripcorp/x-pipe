package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.Redis;

/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public class RedisComparator extends AbstractMetaComparator<Object>{

    //todo, to be deleted

	private Redis current, future;
	
	public RedisComparator(Redis current, Redis future) {
		this.current = current;
		this.future = future;
	}

	@Override
	public void compare() {
		//too many redis meta, avoid reflection
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
