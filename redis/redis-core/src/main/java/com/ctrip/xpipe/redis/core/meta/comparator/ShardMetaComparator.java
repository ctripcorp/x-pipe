package com.ctrip.xpipe.redis.core.meta.comparator;


import com.ctrip.xpipe.redis.core.entity.InstanceNode;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.tuple.Pair;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public class ShardMetaComparator extends AbstractInstanceNodeComparator{

	private ShardMeta current, future;

	public ShardMetaComparator(ShardMeta current, ShardMeta future){
		this.current = current;
		this.future = future;
	}

	@Override
	public void compare() {
		configChanged = checkShallowChange(current, future);

		List<InstanceNode> currentAll =  getAll(current);
		List<InstanceNode> futureAll =  getAll(future);

		Pair<List<InstanceNode>, List<Pair<InstanceNode, InstanceNode>>> subResult = sub(futureAll, currentAll);
		List<InstanceNode> tAdded = subResult.getKey();
		added.addAll(tAdded);

		List<Pair<InstanceNode, InstanceNode>> modified = subResult.getValue();
		compareConfigConfig(modified);
		
		List<InstanceNode> tRemoved = sub(currentAll, futureAll).getKey();
		removed.addAll(tRemoved);
	}


	private List<InstanceNode> getAll(ShardMeta shardMeta) {
		
		List<InstanceNode> result = new LinkedList<>();
		result.addAll(shardMeta.getRedises());
		result.addAll(shardMeta.getKeepers());
		result.addAll(shardMeta.getAppliers());

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
