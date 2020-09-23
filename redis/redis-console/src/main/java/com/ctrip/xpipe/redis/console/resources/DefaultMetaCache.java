package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManager;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
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
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultMetaCache implements MetaCache {

    private int refreshIntervalMilli = 2000, DEFAULT_KEEPER_NUMBERS = 3 * 10000;

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static final String CONSOLE_IDC = FoundationService.DEFAULT.getDataCenter();

    @Autowired
    private DcMetaService dcMetaService;

    @Autowired
    private DcService dcService;

    @Autowired
    private ConsoleConfig consoleConfig;

    private Pair<XpipeMeta, XpipeMetaManager> meta;

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);

    private Map<String, Pair<String, String>> monitor2ClusterShard;

    private Set<HostPort> allKeepers;

    private int allKeeperSize = DEFAULT_KEEPER_NUMBERS;

    private volatile long lastUpdateTime;

    public DefaultMetaCache() {

    }

    @PostConstruct
    public void postConstruct() {

        logger.info("[postConstruct]{}", this);

        refreshIntervalMilli = consoleConfig.getCacheRefreshInterval();

        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                loadCache();
            }

        }, 1000, refreshIntervalMilli, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void shutdown() {
        if(scheduled != null) {
            scheduled.shutdownNow();
        }
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

                XpipeMeta xpipeMeta = createXpipeMeta(dcMetas);
                Pair<XpipeMeta, XpipeMetaManager> meta = new Pair<>(xpipeMeta, new DefaultXpipeMetaManager(xpipeMeta));
                DefaultMetaCache.this.meta = meta;
                monitor2ClusterShard = Maps.newHashMap();
                allKeeperSize = allKeepers == null ? DEFAULT_KEEPER_NUMBERS : allKeepers.size();
                allKeepers = null;
                lastUpdateTime = System.currentTimeMillis();
            }
        });
    }

    @Override
    public XpipeMeta getXpipeMeta() {
        try {
            return meta.getKey();
        } catch (Exception e) {
            logger.debug("[getXpipeMeta]", e);
        }
        return null;
    }


    private XpipeMeta createXpipeMeta(List<DcMeta> dcMetas){

        XpipeMeta xpipeMeta = new XpipeMeta();
        for (DcMeta dcMeta : dcMetas) {
            xpipeMeta.addDc(dcMeta);
        }
        return xpipeMeta;

    }

    @Override
    public boolean inBackupDc(HostPort hostPort) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();
        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);
        if (metaDesc == null) {
            throw new IllegalStateException("unfound shard for instance:" + hostPort);
        }

        String instanceInDc = metaDesc.getDcId();
        String activeDc = metaDesc.getActiveDc();
        return !activeDc.equalsIgnoreCase(instanceInDc);
    }

    @Override
    public HostPort findMasterInSameShard(HostPort hostPort) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();
        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);
        if (metaDesc == null) {
            throw new IllegalStateException("unfound shard for instance:" + hostPort);
        }

        String clusterName = metaDesc.getClusterId();
        String shardName = metaDesc.getShardId();

        Pair<String, RedisMeta> redisMaster = xpipeMetaManager.getRedisMaster(clusterName, shardName);
        // could be null if no master in a shard
        if(redisMaster == null) {
            return null;
        }
        RedisMeta redisMeta = redisMaster.getValue();
        return new HostPort(redisMeta.getIp(), redisMeta.getPort());
    }

    @Override
    public Pair<String, String> findClusterShard(HostPort hostPort) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();

        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);
        if (metaDesc == null) {
            return null;
        }

        return new Pair<>(metaDesc.getClusterId(), metaDesc.getShardId());
    }

    @Override
    public String getActiveDc(HostPort hostPort) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();

        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);
        if (metaDesc == null) {
            return null;
        }

        return metaDesc.getActiveDc();
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public RouteMeta getRouteIfPossible(HostPort hostPort) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);
        if (metaDesc == null) {
            logger.warn("[getRouteIfPossible]HostPort corresponding meta not found: {}", hostPort);
            return null;
        }
        return xpipeMetaManager
                .consoleRandomRoute(CONSOLE_IDC, XpipeMetaManager.ORG_ID_FOR_SHARED_ROUTES, metaDesc.getDcId());
    }

    @Override
    public boolean isCrossRegion(String activeDc, String backupDc) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();
        return !xpipeMetaManager.getDcZone(activeDc)
                .equalsIgnoreCase(xpipeMetaManager.getDcZone(backupDc));
    }

    @Override
    public List<HostPort> getAllActiveRedisOfDc(String activeDc, String dcId) {
        List<HostPort> result = Lists.newLinkedList();
        boolean isDcActiveDc = activeDc.equalsIgnoreCase(dcId);
        try {
            for(ClusterMeta clusterMeta : meta.getKey().findDc(dcId).getClusters().values()) {
                ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
                if (clusterType.supportSingleActiveDC() && !clusterMeta.getActiveDc().equalsIgnoreCase(activeDc)) {
                    continue;
                }
                if (clusterType.supportMultiActiveDC() && !isDcActiveDc) {
                    continue;
                }
                for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                    for(RedisMeta redis : shardMeta.getRedises()) {
                        result.add(new HostPort(redis.getIp(), redis.getPort()));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("[getRedisNumOfDc]", e);
        }
        return result;
    }

    @Override
    public Set<HostPort> getAllKeepers(){
        XpipeMeta xpipeMeta = getXpipeMeta();
        if (allKeepers == null) {
            synchronized (this) {
                if (allKeepers == null) {
                    Set<HostPort> localKeepers = Sets.newHashSetWithExpectedSize(allKeeperSize);
                    xpipeMeta.getDcs().forEach((dcName, dcMeta) -> {
                        dcMeta.getClusters().forEach((clusterName, clusterMeta) -> {
                            clusterMeta.getShards().forEach((shardName, shardMeta) -> {
                                shardMeta.getKeepers().forEach(keeperMeta -> {
                                    localKeepers.add(new HostPort(keeperMeta.getIp(), keeperMeta.getPort()));
                                });
                            });
                        });
                    });
                    allKeepers = localKeepers;
                }
            }
        }

        return allKeepers;
    }


    @Override
    public String getSentinelMonitorName(String clusterId, String shardId) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();

        String activeDc = xpipeMetaManager.getActiveDc(clusterId, shardId);
        return xpipeMetaManager.getSentinelMonitorName(activeDc, clusterId, shardId);
    }

    @Override
    public Set<HostPort> getActiveDcSentinels(String clusterId, String shardId) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();

        String activeDc = xpipeMetaManager.getActiveDc(clusterId, shardId);
        SentinelMeta sentinel = xpipeMetaManager.getSentinel(activeDc, clusterId, shardId);

        return new HashSet<>(IpUtils.parseAsHostPorts(sentinel.getAddress()));
    }

    @Override
    public HostPort findMaster(String clusterId, String shardId) throws MasterNotFoundException {

        XpipeMetaManager xpipeMetaManager = meta.getValue();
        Pair<String, RedisMeta> redisMaster = xpipeMetaManager.getRedisMaster(clusterId, shardId);
        if (redisMaster == null) {
            throw new MasterNotFoundException(clusterId, shardId);
        }
        return new HostPort(redisMaster.getValue().getIp(), redisMaster.getValue().getPort());
    }

    @Override
    public String getDc(HostPort hostPort) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();
        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);

        if (metaDesc == null) {
            throw new IllegalStateException("unfound shard for instance:" + hostPort);
        }
        return metaDesc.getDcId();
    }

    @Override
    public Pair<String, String> findClusterShardBySentinelMonitor(String monitor) {
        if(StringUtil.isEmpty(monitor)) {
            return null;
        }

        Pair<String, String> clusterShard = monitor2ClusterShard.get(monitor);
        if(clusterShard != null) {
            return clusterShard;
        }

        synchronized (this) {
            loadSentinelMonitorInfo();
        }
        return monitor2ClusterShard.get(monitor);
    }

    private void loadSentinelMonitorInfo() {
        try {
            XpipeMeta xpipeMeta = meta.getKey();
            for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
                for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                    // pass by non-active dc meta
                    if (!clusterMeta.getActiveDc().equals(dcMeta.getId())) {
                        continue;
                    }
                    for (ShardMeta shardMeta : clusterMeta.getShards().values()) {

                        monitor2ClusterShard.put(shardMeta.getSentinelMonitorName(),
                                new Pair<>(clusterMeta.getId(), shardMeta.getId()));

                    }
                }
            }
        } catch (Exception e) {
            logger.error("[findClusterShardBySentinelMonitor]", e);
        }
    }

    @Override
    public String getActiveDc(String clusterId, String shardId){
        XpipeMetaManager xpipeMetaManager  =  meta.getValue();
        return xpipeMetaManager.getActiveDc(clusterId, shardId);
    }

    @Override
    public ClusterType getClusterType(String clusterId) {
        XpipeMeta xpipeMeta = meta.getKey();
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            if (!dcMeta.getClusters().containsKey(clusterId)) continue;
            return ClusterType.lookup(dcMeta.getClusters().get(clusterId).getType());
        }

        throw new IllegalStateException("[getClusterType] unfound cluster for name:" + clusterId);
    }

    @VisibleForTesting
    protected DefaultMetaCache setMeta(Pair<XpipeMeta, XpipeMetaManager> meta) {
        this.meta = meta;
        return this;
    }
}
