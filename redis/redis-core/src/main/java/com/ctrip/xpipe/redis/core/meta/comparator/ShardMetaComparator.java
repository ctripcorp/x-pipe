package com.ctrip.xpipe.redis.core.meta.comparator;


import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.entity.Redis;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.tuple.Pair;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public class ShardMetaComparator extends AbstractMetaComparator<Redis, ShardChange>{
	
	private ShardMeta current, future;
	
	public ShardMetaComparator(ShardMeta current, ShardMeta future){
		this.current = current;
		this.future = future;
	}

	@Override
	public void compare() {
		List<Redis> currentAll =  getAll(current);
		List<Redis> futureAll =  getAll(future);


		Pair<List<Redis>, List<Pair<Redis, Redis>>> subResult = sub(futureAll, currentAll);
		List<Redis> tAdded = subResult.getKey();
		added.addAll(tAdded);

		List<Pair<Redis, Redis>> modified = subResult.getValue();
		compareConfigConfig(modified);
		
		List<Redis> tRemoved = sub(currentAll, futureAll).getKey();
		removed.addAll(tRemoved);
	}

	private void compareConfigConfig(List<Pair<Redis, Redis>> allModified) {
		
		for(Pair<Redis, Redis> pair : allModified){
			Redis current = pair.getValue();
			Redis future = pair.getKey();
			if(current.equals(future)){
				continue;
			}
			RedisComparator redisComparator = new RedisComparator(current, future);
			redisComparator.compare();
			modified.add(redisComparator);
		}
		
	}

	private Pair<List<Redis>, List<Pair<Redis, Redis>>> sub(List<Redis> all1, List<Redis> all2) {
		
		List<Redis> subResult = new LinkedList<>();
		List<Pair<Redis, Redis>> intersectResult = new LinkedList<>();
		
		for(Redis redis1 : all1){
			
			Redis redis2Equal = null;
			for(Redis redis2 : all2){
				if(MetaUtils.theSame(redis1, redis2)){
					redis2Equal = redis2;
					break;
				}
			}
			if(redis2Equal == null){
				subResult.add(redis1);
			}else{
				intersectResult.add(new Pair<>(redis1, redis2Equal));
			}
		}
		return new Pair<List<Redis>, List<Pair<Redis, Redis>>>(subResult, intersectResult);
	}

	private List<Redis> getAll(ShardMeta shardMeta) {
		
		List<Redis> result = new LinkedList<>();
		result.addAll(shardMeta.getRedises());
		result.addAll(shardMeta.getKeepers());
		return result;
	}
	
	
	public ShardMeta getCurrent() {
		return current;
	}
	
	public ShardMeta getFuture() {
		return future;
	}

	@Override
	public String idDesc() {
		return current.getId();
	}

}
