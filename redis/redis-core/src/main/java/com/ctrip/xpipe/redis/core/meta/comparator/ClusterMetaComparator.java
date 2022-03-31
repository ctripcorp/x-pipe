package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.SourceMeta;
import org.unidal.tuple.Triple;

import java.util.HashSet;
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

		compareSourceShard();

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

	@Override
	public String idDesc() {
		return current.getId();
	}

	private void compareSourceShard() {

		Set<String> currentShardMetaSet = new HashSet<>();
		current.getSources().forEach(t -> currentShardMetaSet.addAll(t.getShards().keySet()));

		Set<String> futureShardMetaSet = new HashSet<>();
		future.getSources().forEach(t -> futureShardMetaSet.addAll(t.getShards().keySet()));

		Triple<Set<String>, Set<String>, Set<String>> result = getDiff(currentShardMetaSet, futureShardMetaSet);

		for (String shardId : result.getFirst()) {
			ShardMeta shardMeta = findSourceShard(shardId, future);
			if (shardMeta != null) {
				added.add(shardMeta);
			}
		}

		for (String shardId : result.getLast()) {
			ShardMeta shardMeta = findSourceShard(shardId, current);
			if (shardMeta != null) {
				removed.add(shardMeta);
			}
		}

		for (String shardId : result.getMiddle()) {
			ShardMeta currentMeta = findSourceShard(shardId, current);
			ShardMeta futureMeta = findSourceShard(shardId, future);
			if (!reflectionEquals(currentMeta, futureMeta)) {
				ShardMetaComparator comparator = new ShardMetaComparator(currentMeta, futureMeta);
				comparator.compare();
				modified.add(comparator);
			}
		}
	}

	private ShardMeta findSourceShard(String shardId, ClusterMeta clusterMeta) {

		for (SourceMeta sourceMeta : clusterMeta.getSources()) {
			ShardMeta shardMeta = sourceMeta.findShard(shardId);
			if (shardMeta != null) {
				return shardMeta;
			}
		}

		return null;

	}
}
