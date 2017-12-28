package com.ctrip.xpipe.redis.core.meta.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wenchao.meng
 *         <p>
 *         Dec 28, 2017
 */
public class InverseHostPortMapBuilder {

    private static Logger logger = LoggerFactory.getLogger(InverseHostPortMapBuilder.class);

    public static Map<HostPort, XpipeMetaManager.MetaDesc> build(XpipeMeta xpipeMeta) {

        logger.info("build reverse map");

        Map<HostPort, XpipeMetaManager.MetaDesc> result = new ConcurrentHashMap();

        xpipeMeta.getDcs().forEach((dc, dcMeta) -> {

            dcMeta.getClusters().forEach((clusterId, clusterMeta) -> {

                clusterMeta.getShards().forEach((shardId, shardMeta) -> {

                    for(RedisMeta redisMeta : shardMeta.getRedises()){
                        result.put(new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                                new XpipeMetaManager.MetaDesc(dcMeta, clusterMeta, shardMeta, redisMeta));
                    }

                    for(KeeperMeta keeperMeta : shardMeta.getKeepers()){
                        result.put(new HostPort(keeperMeta.getIp(), keeperMeta.getPort()),
                                new XpipeMetaManager.MetaDesc(dcMeta, clusterMeta, shardMeta, keeperMeta));
                    }
                });
            });

        });

        return result;
    }


}
