package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManager;
import com.ctrip.xpipe.utils.IpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;

import javax.annotation.PostConstruct;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 31, 2017
 */
@Component
@Lazy
public class DefaultMetaCache implements MetaCache {

    private int refreshIntervalMilli = 2000;

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private DcMetaService dcMetaService;

    @Autowired
    private DcService dcService;

    @Autowired
    private ConsoleConfig consoleConfig;

    private List<DcMeta> dcMetas;

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);

    public DefaultMetaCache() {

    }

    @PostConstruct
    public void postConstruct() {


        refreshIntervalMilli = consoleConfig.getCacheRefreshInterval();

        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                loadCache();
            }
        }, 1000, refreshIntervalMilli, TimeUnit.MILLISECONDS);
    }

    private void loadCache() throws Exception {


        TransactionMonitor.DEFAULT.logTransaction("MetaCache", "load", new Task() {

            @Override
            public void go() throws Exception {

                List<DcTbl> dcs = dcService.findAllDcNames();
                List<DcMeta> dcMetas = new LinkedList<>();
                for (DcTbl dc : dcs) {
                    dcMetas.add(dcMetaService.getDcMeta(dc.getDcName()));
                }

                DefaultMetaCache.this.dcMetas = dcMetas;
            }
        });
    }

    @Override
    public XpipeMeta getXpipeMeta() {

        if (dcMetas == null) {
            try {
                loadCache();
            } catch (Exception e) {
                logger.error("[getXpipeMeta]", e);
                return null;
            }
        }

        XpipeMeta xpipeMeta = new XpipeMeta();
        for (DcMeta dcMeta : dcMetas) {
            xpipeMeta.addDc(dcMeta);
        }
        return xpipeMeta;
    }

    @Override
    public boolean inBackupDc(HostPort hostPort) {

        XpipeMeta xpipeMeta = getXpipeMeta();

        XpipeMetaManager xpipeMetaManager = new DefaultXpipeMetaManager(xpipeMeta);
        ShardMeta shardMeta = xpipeMetaManager.findShardMeta(hostPort);
        if (shardMeta == null) {
            throw new IllegalStateException("unfound shard for instance:" + hostPort);
        }
        String instanceInDc = shardMeta.parent().parent().getId();
        String activeDc = shardMeta.getActiveDc();
        return !activeDc.equalsIgnoreCase(instanceInDc);
    }

    @Override
    public HostPort findMasterInSameShard(HostPort hostPort) {

        XpipeMetaManager xpipeMetaManager = new DefaultXpipeMetaManager(getXpipeMeta());

        ShardMeta currentShard = xpipeMetaManager.findShardMeta(hostPort);
        if (currentShard == null) {
            throw new IllegalStateException("unfound shard for instance:" + hostPort);
        }

        String clusterName = currentShard.parent().getId();
        String shardName = currentShard.getId();

        Pair<String, RedisMeta> redisMaster = xpipeMetaManager.getRedisMaster(clusterName, shardName);
        RedisMeta redisMeta = redisMaster.getValue();
        return new HostPort(redisMeta.getIp(), redisMeta.getPort());
    }

    @Override
    public Pair<String, String> findClusterShard(HostPort hostPort) {

        XpipeMetaManager xpipeMetaManager = new DefaultXpipeMetaManager(getXpipeMeta());

        ShardMeta currentShard = xpipeMetaManager.findShardMeta(hostPort);
        if (currentShard == null) {
            return null;
        }

        String clusterName = currentShard.parent().getId();
        String shardName = currentShard.getId();

        return new Pair<>(clusterName, shardName);
    }

    @Override
    public String getSentinelMonitorName(String clusterId, String shardId) {

        XpipeMetaManager xpipeMetaManager = new DefaultXpipeMetaManager(getXpipeMeta());
        Set<String> dcs = xpipeMetaManager.getDcs();
        for (String dc : dcs) {
            ShardMeta shardMeta = xpipeMetaManager.getShardMeta(dc, clusterId, shardId);
            if (shardMeta != null) {
                return shardMeta.getSentinelMonitorName();
            }
        }
        return null;
    }

    @Override
    public Set<HostPort> getActiveDcSentinels(String clusterId, String shardId) {

        XpipeMetaManager xpipeMetaManager = new DefaultXpipeMetaManager(getXpipeMeta());
        String activeDc = xpipeMetaManager.getActiveDc(clusterId, shardId);
        SentinelMeta sentinel = xpipeMetaManager.getSentinel(activeDc, clusterId, shardId);

        return new HashSet<>(IpUtils.parseAsHostPorts(sentinel.getAddress()));
    }

    @Override
    public HostPort findMaster(String clusterId, String shardId) throws MasterNotFoundException {

        XpipeMetaManager xpipeMetaManager = new DefaultXpipeMetaManager(getXpipeMeta());
        Pair<String, RedisMeta> redisMaster = xpipeMetaManager.getRedisMaster(clusterId, shardId);
        if (redisMaster == null) {
            throw new MasterNotFoundException(clusterId, shardId);
        }
        return new HostPort(redisMaster.getValue().getIp(), redisMaster.getValue().getPort());
    }

}
