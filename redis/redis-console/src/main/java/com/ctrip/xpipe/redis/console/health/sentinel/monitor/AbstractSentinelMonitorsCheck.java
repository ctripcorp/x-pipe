package com.ctrip.xpipe.redis.console.health.sentinel.monitor;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.health.AbstractIntervalCheck;
import com.ctrip.xpipe.redis.console.redis.SentinelManager;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.utils.IpUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collection;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Feb 23, 2018
 */

public abstract class AbstractSentinelMonitorsCheck extends AbstractIntervalCheck {

    @Autowired
    protected MetaCache metaCache;

    @Autowired
    protected SentinelManager sentinelManager;

    @Override
    public void doCheck() {
        logger.info("[doCheck] check sentinel monitors");
        Collection<DcMeta> dcMetas = metaCache.getXpipeMeta().getDcs().values();
        for(DcMeta dcMeta : dcMetas) {
            Collection<SentinelMeta> sentinelMetas = dcMeta.getSentinels().values();
            for(SentinelMeta sentinelMeta : sentinelMetas) {
                List<HostPort> sentinels = IpUtils.parseAsHostPorts(sentinelMeta.getAddress());
                for(HostPort hostPort : sentinels) {
                    checkSentinel(sentinelMeta, hostPort);
                }
            }
        }
    }

    protected abstract void checkSentinel(SentinelMeta sentinelMeta, HostPort hostPort);
}
