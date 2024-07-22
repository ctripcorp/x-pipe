package com.ctrip.xpipe.redis.console.healthcheck.session;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.*;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

@Component
public class DefaultRedisConsoleSessionManager  extends AbstractConsoleInstanceSessionManager implements RedisConsoleSessionManager {

    @Override
    protected Set<HostPort> getInUseInstances() {
        Set<HostPort> redisInUse = new HashSet<>();
        List<DcMeta> dcMetas = new LinkedList<>(metaCache.getXpipeMeta().getDcs().values());
        if(dcMetas.isEmpty())	return null;
        for (DcMeta dcMeta : dcMetas) {
            if(dcMeta == null)	break;
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

}
