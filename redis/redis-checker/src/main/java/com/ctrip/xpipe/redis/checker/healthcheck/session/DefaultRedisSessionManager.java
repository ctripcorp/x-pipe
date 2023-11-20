package com.ctrip.xpipe.redis.checker.healthcheck.session;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.*;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static com.ctrip.xpipe.redis.core.meta.comparator.KeeperContainerMetaComparator.getMonitorRedisMeta;

/**
 * @author marsqing
 *
 *         Dec 1, 2016 6:42:01 PM
 */

@Component
public class DefaultRedisSessionManager extends AbstractInstanceSessionManager implements RedisSessionManager {

	@Override
	protected Set<HostPort> getInUseInstances() {
		Set<HostPort> redisInUse = new HashSet<>();
		List<DcMeta> dcMetas = new LinkedList<>(metaCache.getXpipeMeta().getDcs().values());
		if(dcMetas.isEmpty())	return null;
		for (DcMeta dcMeta : dcMetas) {
			if(dcMeta == null)	break;

			if (dcMeta.getId().equalsIgnoreCase(currentDcId)) {
				redisInUse.addAll(getSessionsForKeeper(dcMeta, currentDcAllMeta.getCurrentDcAllMeta()));
			}

			for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
				for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
					for (RedisMeta redisMeta : shardMeta.getRedises()) {
						redisInUse.add(new HostPort(redisMeta.getIp(), redisMeta.getPort()));
					}
				}
			}
		}
		return redisInUse;
	}

	@Override
	protected HostPort getMonitorInstance(List<RedisMeta> redises, KeeperMeta keeper) {
		RedisMeta monitorRedisMeta = getMonitorRedisMeta(redises);
		return monitorRedisMeta == null ? null : new HostPort(monitorRedisMeta.getIp(), monitorRedisMeta.getPort());
	}
}
