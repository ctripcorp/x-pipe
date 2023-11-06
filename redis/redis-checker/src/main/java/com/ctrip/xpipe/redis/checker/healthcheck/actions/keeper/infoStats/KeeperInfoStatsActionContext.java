package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.infoStats;

import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractInfoContext;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;


public class KeeperInfoStatsActionContext extends AbstractInfoContext<KeeperHealthCheckInstance> {

    public KeeperInfoStatsActionContext(KeeperHealthCheckInstance instance, String extractor) {
        super(instance, new InfoResultExtractor(extractor));
    }
}
