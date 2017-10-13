package com.ctrip.xpipe.redis.meta.server.cluster.impl;


import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.observer.NodeModified;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.cluster.*;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.ZkUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 22, 2016
 */
public class AbstractClusterServers<T extends ClusterServer> extends AbstractLifecycleObservable implements ClusterServers<T>, TopElement {

    private Map<Integer, T> servers = new ConcurrentHashMap<>();

    @Autowired
    private MetaServerConfig metaServerConfig;

    @Autowired
    private ZkClient zkClient;

    @Autowired
    private T currentServer;

    private PathChildrenCache serversCache;

    @Autowired
    private RemoteClusterServerFactory<T> remoteClusterServerFactory;

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create(getClass().getSimpleName()));

    private ScheduledFuture<?> future;

    @Override
    protected void doInitialize() throws Exception {

    }

    @Override
    protected void doStart() throws Exception {

        CuratorFramework client = zkClient.get();

        serversCache = new PathChildrenCache(client, MetaZkConfig.getMetaServerRegisterPath(), true,
        XpipeThreadFactory.create(String.format("PathChildrenCache(%d)", currentServer.getServerId())));
        serversCache.getListenable().addListener(new ChildrenChanged());
        serversCache.start();

        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            public void doRun() {
                try {
                    childrenChanged();
                } catch (Throwable th) {
                    logger.error("[doStart]", th);
                }

            }
        }, 1000, metaServerConfig.getClusterServersRefreshMilli(), TimeUnit.MILLISECONDS);

    }

    protected class ChildrenChanged implements PathChildrenCacheListener {

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {

            logger.info("[childEvent]{}, {}", event.getType(), ZkUtils.toString(event.getData()));
            childrenChanged();
        }
    }

    @Override
    public T currentClusterServer() {

        return currentServer;
    }

    @Override
    public T getClusterServer(int serverId) {
        return servers.get(serverId);
    }

    @Override
    public int getOrder() {
        return 0;
    }

    //for test
    public String getServerIdFromPath(String path, String serverBasePath) {

        int index = path.indexOf(serverBasePath);
        if (index >= 0) {
            path = path.substring(index + serverBasePath.length());
        }
        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        return path;
    }

    private synchronized void childrenChanged() throws ClusterException {

        try {
            logger.info("[childrenChanged][start][{}]{}", currentServerId(), servers);
            List<ChildData> allServers = serversCache.getCurrentData();
            String serverBasePath = MetaZkConfig.getMetaServerRegisterPath();

            Set<Integer> currentServers = new HashSet<>();
            for (ChildData childData : allServers) {

                String serverIdStr = getServerIdFromPath(childData.getPath(), serverBasePath);
                int serverId = Integer.parseInt(serverIdStr);
                byte[] data = childData.getData();
                ClusterServerInfo info = Codec.DEFAULT.decode(data, ClusterServerInfo.class);

                logger.debug("[childrenChanged][{}]{},{}", currentServerId(), serverId, info);
                currentServers.add(serverId);

                ClusterServer server = servers.get(serverId);
                if (server == null) {
                    logger.info("[childrenChanged][{}][createNew]{}{}", currentServerId(), serverId, info);
                    T remoteServer = remoteClusterServerFactory.createClusterServer(serverId, info);
                    servers.put(serverId, remoteServer);
                    logger.info("[childrenChanged][{}][createNew]{}", currentServerId(), servers);
                    serverAdded(remoteServer);
                } else {
                    if (!info.equals(server.getClusterInfo())) {
                        logger.info("[childrenChanged][{}][clusterInfoChanged]{}{}", currentServerId(), serverId, info, server.getClusterInfo());
                        T newServer = remoteClusterServerFactory.createClusterServer(serverId, info);
                        servers.put(serverId, newServer);
                        serverChanged(server, newServer);
                    }
                }
            }

            for (Integer old : servers.keySet()) {
                if (!currentServers.contains(old)) {
                    ClusterServer serverInfo = servers.remove(old);
                    logger.info("[childrenChanged][remote not exist][{}]{}, {}, current:{}", currentServerId(), old, serverInfo);
                    remoteDelted(serverInfo);

                }
            }

            logger.info("[childrenChanged][ end ][{}]{}", currentServerId(), servers);
        } catch (Exception e) {
            throw new ClusterException("[childrenChanged]", e);
        }
    }


    private Object currentServerId() {
        return currentServer.getServerId();
    }

    private void remoteDelted(ClusterServer serverInfo) {
        notifyObservers(new NodeDeleted<ClusterServer>(serverInfo));
    }

    private void serverChanged(ClusterServer oldServer, ClusterServer newServer) {
        notifyObservers(new NodeModified<ClusterServer>(oldServer, newServer));
    }

    private void serverAdded(ClusterServer remoteServer) {
        notifyObservers(new NodeAdded<ClusterServer>(remoteServer));
    }

    public void setMetaServerConfig(MetaServerConfig metaServerConfig) {
        this.metaServerConfig = metaServerConfig;
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public void setCurrentServer(T currentServer) {
        this.currentServer = currentServer;
    }

    public void setRemoteClusterServerFactory(RemoteClusterServerFactory<T> remoteClusterServerFactory) {
        this.remoteClusterServerFactory = remoteClusterServerFactory;
    }

    @Override
    public Set<T> allClusterServers() {
        return new HashSet<>(servers.values());
    }


    @Override
    protected void doStop() throws Exception {

        serversCache.close();
        if (future != null) {
            future.cancel(true);
            future = null;
        }
    }

    @Override
    public void refresh() throws ClusterException {
        childrenChanged();
    }

    @Override
    public boolean exist(int serverId) {
        return servers.get(serverId) != null;
    }

    @Override
    public Map<Integer, ClusterServerInfo> allClusterServerInfos() {

        Map<Integer, ClusterServerInfo> result = new HashMap<>();
        for (Entry<Integer, T> entry : servers.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getClusterInfo());
        }
        return result;
    }


}
