package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import org.unidal.tuple.Triple;

import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public class ClusterMetaComparator extends AbstractMetaComparator<ShardMeta, ClusterChange>{
	
	private ClusterMeta current, future;

	private boolean metaChange;
	
	public ClusterMetaComparator(ClusterMeta current, ClusterMeta future) {
		this.current = current;
		this.future = future;
	}

	@Override
	public void compare() {

		ClusterMeta currentClone = MetaClone.clone(current);
		ClusterMeta futureClone = MetaClone.clone(future);
		currentClone.getShards().clear();
		futureClone.getShards().clear();
		metaChange = !(currentClone.toString().equals(futureClone.toString()));

		Triple<Set<String>, Set<String>, Set<String>> result = getDiff(current.getShards().keySet(), future.getShards().keySet());
		
		for(String shardId : result.getFirst()){
			added.add(future.findShard(shardId));
		}

		for(String shardId : result.getLast()){
			removed.add(current.findShard(shardId));
		}

		for(String shardId : result.getMiddle()){
			ShardMeta currentMeta = current.findShard(shardId);
			ShardMeta futureMeta = future.findShard(shardId);
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

	public boolean metaChange() {
		return metaChange;
	}

	@Override
	public String idDesc() {
		return current.getId();
	}
}
