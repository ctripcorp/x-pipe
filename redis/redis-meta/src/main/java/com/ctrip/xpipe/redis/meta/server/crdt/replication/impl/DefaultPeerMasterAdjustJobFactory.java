package com.ctrip.xpipe.redis.meta.server.crdt.replication.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.meta.server.crdt.replication.PeerMasterAdjustJobFactory;
import com.ctrip.xpipe.redis.meta.server.job.PeerMasterAdjustJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig.CLIENT_POOL;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

@Component
public class DefaultPeerMasterAdjustJobFactory implements PeerMasterAdjustJobFactory {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected DcMetaCache dcMetaCache;

    protected CurrentMetaManager currentMetaManager;

    private Executor executors;

    protected ScheduledExecutorService scheduled;

    private XpipeNettyClientKeyedObjectPool keyedObjectPool;


    @Autowired
    public DefaultPeerMasterAdjustJobFactory(DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager,
                                             @Qualifier(CLIENT_POOL) XpipeNettyClientKeyedObjectPool keyedObjectPool,
                                             @Qualifier(GLOBAL_EXECUTOR) Executor executors) {
        this.dcMetaCache = dcMetaCache;
        this.currentMetaManager = currentMetaManager;
        this.keyedObjectPool = keyedObjectPool;
        this.executors = executors;

        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("PeerMasterAdjustJobSchedule"));
    }

    @Override
    public PeerMasterAdjustJob buildPeerMasterAdjustJob(Long clusterDbId, Long shardDbId) {
        Set<String> upstreamPeerDcs = currentMetaManager.getUpstreamPeerDcs(clusterDbId, shardDbId);
        if (upstreamPeerDcs.isEmpty()) {
            logger.info("[buildPeerMasterAdjustJob][{}][{}] unknown any upstream dcs, skip adjust", clusterDbId, shardDbId);
            return null;
        }

        RedisMeta currentMaster = currentMetaManager.getCurrentCRDTMaster(clusterDbId, shardDbId);
        if (null == currentMaster) {
            logger.info("[buildPeerMasterAdjustJob][{}][{}] unknown current master, skip adjust", clusterDbId, shardDbId);
            return null;
        }
        List<Pair<Long, Endpoint>> allPeerMasters = currentMetaManager.getAllPeerMasters(clusterDbId, shardDbId).entrySet().stream().map(entry -> {
            String dcName = entry.getKey();
            RedisMeta peerMeta = entry.getValue();
            RouteMeta routeMeta = currentMetaManager.getClusterRouteByDcId(dcName, clusterDbId);
            Endpoint endpoint;
            if(routeMeta != null) {
                ProxyConnectProtocol proxyProtocol =  new DefaultProxyConnectProtocolParser().read(String.format("%s %s %s", ProxyConnectProtocol.KEY_WORD, PROXY_OPTION.ROUTE, routeMeta.getRouteInfo()));
                endpoint = new ProxyEnabledEndpoint(peerMeta.getIp(), peerMeta.getPort(), proxyProtocol);
            } else {
                endpoint = new DefaultEndPoint(peerMeta.getIp(), peerMeta.getPort());
            }
            return new Pair<>(entry.getValue().getGid(), endpoint);
        }).collect(Collectors.toList());
        return new PeerMasterAdjustJob(clusterDbId, shardDbId, allPeerMasters,
                Pair.of(currentMaster.getIp(), currentMaster.getPort()), false,
                keyedObjectPool.getKeyPool(new DefaultEndPoint(currentMaster.getIp(), currentMaster.getPort())),
                scheduled, executors);
    }

}
