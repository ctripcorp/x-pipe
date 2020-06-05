package com.ctrip.xpipe.redis.console.healthcheck.nonredis.monitor;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.dao.ShardDao;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.AbstractCrossDcIntervalCheck;
import com.ctrip.xpipe.redis.console.redis.SentinelManager;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

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
    protected ConsoleDbConfig consoleDbConfig;

    @Autowired
    protected DcService dcService;

    @Autowired
    protected ClusterDao clusterDao;

    @Autowired
    protected ShardDao shardDao;

    protected Set<String> monitorNames = null;

    @Override
    public void doCheck() {
        logger.debug("[doCheck] check sentinel monitors");

        refreshMonitorNames();
        if (null == monitorNames) {
            logger.info("[doCheck] skip check for init monitor names fail");
            return;
        }

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
        monitorNames = null;
    }

    protected List<DcMeta> dcsToCheck() {
        List<DcMeta> result = new LinkedList<>(metaCache.getXpipeMeta().getDcs().values());
        Set<String> ignoredDcNames = consoleConfig.getIgnoredHealthCheckDc();
        List<DcMeta> toRemove = Lists.newArrayList();
        for(DcMeta dcMeta : result) {
            if (ignoredDcNames.contains(dcMeta.getId()) || ignoredDcNames.contains(dcMeta.getId().toUpperCase())) {
                toRemove.add(dcMeta);
            }
        }
        result.removeAll(toRemove);
        return result;
    }

    protected boolean checkMonitorName(String name) {
        return null != monitorNames && monitorNames.contains(name);
    }

    private void refreshMonitorNames() {
        try {
            monitorNames = new GetSentinelMonitorNamesCommand(dcService, clusterDao, shardDao, executors, scheduled).execute().get();
        } catch (Exception e) {
            monitorNames = null;
            logger.info("[initMonitorNames] fail", e);
        }
    }

    @Override
    protected boolean shouldCheck() {
        return super.shouldCheck() && consoleDbConfig.isSentinelAutoProcess();
    }

    protected abstract void checkSentinel(SentinelMeta sentinelMeta, HostPort hostPort);

    @VisibleForTesting
    protected void setMonitorNames(Set<String> monitorNames) {
        this.monitorNames = monitorNames;
    }
}
