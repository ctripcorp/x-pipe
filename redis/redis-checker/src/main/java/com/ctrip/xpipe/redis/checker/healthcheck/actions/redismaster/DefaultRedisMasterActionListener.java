package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.redis.checker.MetaServerManager;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.HeteroSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;

@Component
public class DefaultRedisMasterActionListener extends AbstractRedisMasterActionListener implements  OneWaySupport, BiDirectionSupport, HeteroSupport {

    private MetaServerManager metaServerManager;

    @Autowired
    public DefaultRedisMasterActionListener(PersistenceCache persistenceCache, MetaCache metaCache,
                                            MetaServerManager metaServerManager) {
        super(persistenceCache, metaCache, Executors.newFixedThreadPool(100, XpipeThreadFactory.create("XPipeRedisMasterJudgement")));
        this.metaServerManager = metaServerManager;
    }

    @Override
    protected RedisMeta finalMaster(String dcId, String clusterId, String shardId) {
        return metaServerManager.getCurrentMaster(dcId, clusterId, shardId);
    }

    @Override
    protected String getServerName() {
        return "meta server";
    }

}
