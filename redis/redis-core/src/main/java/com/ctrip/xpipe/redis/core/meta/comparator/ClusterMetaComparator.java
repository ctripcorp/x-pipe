package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.unidal.tuple.Triple;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public class ClusterMetaComparator extends AbstractMetaComparator<ShardMeta>{

	private ClusterMeta current;

	private ClusterMeta future;

	private Map<Long, ShardMeta> currentGlobalShards;
	private Map<Long, ShardMeta> futureGlobalShards;

	public ClusterMetaComparator(ClusterMeta current, ClusterMeta future) {
		this.current = current;
		this.future = future;
	}

	public void setShardMigrateSupport(Map<Long, ShardMeta> currentGlobalShards, Map<Long, ShardMeta> futureGlobalShards) {
		this.currentGlobalShards = currentGlobalShards;
		this.futureGlobalShards = futureGlobalShards;
	}

	private Map<Long, ShardMeta> extractShardsInCluster(ClusterMeta clusterMeta) {
		return clusterMeta.getAllShards().values().stream()
				.collect(Collectors.toMap(ShardMeta::getDbId, shardMeta -> shardMeta));
	}

	@Override
	public void compare() {
		configChanged = checkShallowChange(current, future);

		Map<Long, ShardMeta> currentShards = extractShardsInCluster(current);
		Map<Long, ShardMeta> futureShards = extractShardsInCluster(future);
		Triple<Set<Long>, Set<Long>, Set<Long>> result = getDiff(currentShards.keySet(), futureShards.keySet());

		for(Long shardId : result.getFirst()){
			// do redundant addAndStart keeper/applier job for shards migrated in
			added.add(futureShards.get(shardId));
		}

		for(Long shardId : result.getLast()){
			if (null != futureGlobalShards && futureGlobalShards.containsKey(shardId)) {
				// for shard migrated out, only release shard manage job but not keeper
				ShardMeta currentMeta = currentShards.get(shardId);
				ShardMetaComparator comparator = new ShardMetaComparator(currentMeta, null);
				modified.add(comparator);
			} else {
				removed.add(currentShards.get(shardId));
			}
		}

		for(Long shardId : result.getMiddle()){
			ShardMeta currentMeta = currentShards.get(shardId);
			ShardMeta futureMeta = futureShards.get(shardId);
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
