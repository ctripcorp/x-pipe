package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.unidal.tuple.Triple;

import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public class ClusterMetaComparator extends AbstractMetaComparator<ShardMeta>{

	private ClusterMeta current;

	private ClusterMeta future;

	public ClusterMetaComparator(ClusterMeta current, ClusterMeta future) {
		this.current = current;
		this.future = future;
	}

	@Override
	public void compare() {
		configChanged = checkShallowChange(current, future);

		Triple<Set<String>, Set<String>, Set<String>> result = getDiff(current.getAllShards().keySet(), future.getAllShards().keySet());

		for(String shardId : result.getFirst()){
			added.add(future.findFromAllShards(shardId));
		}

		for(String shardId : result.getLast()){
			removed.add(current.findFromAllShards(shardId));
		}

		for(String shardId : result.getMiddle()){
			ShardMeta currentMeta = current.findFromAllShards(shardId);
			ShardMeta futureMeta = future.findFromAllShards(shardId);
			if(!reflectionEquals(currentMeta, futureMeta)){
				ShardMetaComparator comparator = new ShardMetaComparator(currentMeta, futureMeta);
				comparator.compare();
				modified.add(comparator);
			}
		}
	}

	public ClusterMeta getCurrent() {
		return current;
	}

	public ClusterMeta getFuture() {
		return future;
	}

	@Override
	public String idDesc() {
		return current.getId();
	}
}
