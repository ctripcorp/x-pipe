package com.ctrip.xpipe.redis.console.healthcheck.nonredis.monitor;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.AbstractCrossDcIntervalCheck;
import com.ctrip.xpipe.redis.console.redis.SentinelManager;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.utils.IpUtils;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Feb 23, 2018
 */

public abstract class AbstractCrossDcSentinelMonitorsCheck extends AbstractCrossDcIntervalCheck {

    @Autowired
    protected MetaCache metaCache;

    @Autowired
    protected SentinelManager sentinelManager;

    @Autowired
    private ConsoleConfig config;

    @Override
    public void doCheck() {
        logger.debug("[doCheck] check sentinel monitors");
        Collection<DcMeta> dcMetas = dcsToCheck();
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

    protected List<DcMeta> dcsToCheck() {
        List<DcMeta> result = new LinkedList<>(metaCache.getXpipeMeta().getDcs().values());
        Set<String> ignoredDcNames = config.getIgnoredHealthCheckDc();
        List<DcMeta> toRemove = Lists.newArrayList();
        for(DcMeta dcMeta : result) {
            if (ignoredDcNames.contains(dcMeta.getId()) || ignoredDcNames.contains(dcMeta.getId().toUpperCase())) {
                toRemove.add(dcMeta);
            }
        }
        result.removeAll(toRemove);
        return result;
    }

    protected abstract void checkSentinel(SentinelMeta sentinelMeta, HostPort hostPort);
}
