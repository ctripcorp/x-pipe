package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.InnerMetaClone;

/**
 * @author lishanglin
 * date 2023/12/13
 */
public class DcMetaClone implements InnerMetaClone<DcMeta> {

    @Override
    public DcMeta clone(DcMeta o) {
        DcMeta clone = new DcMeta();
        clone.setId(o.getId());
        clone.mergeAttributes(o);

        if (null != o.getClusters()) {
            for (ClusterMeta clusterMeta: o.getClusters().values()) {
                ClusterMeta cloneCluster = MetaCloneFacade.INSTANCE.clone(clusterMeta);
                clone.addCluster(cloneCluster);
            }
        }

        if (null != o.getRoutes()) {
            for (RouteMeta routeMeta: o.getRoutes()) {
                RouteMeta cloneRoute = MetaCloneFacade.INSTANCE.clone(routeMeta);
                clone.addRoute(cloneRoute);
            }
        }

        if (null != o.getKeeperContainers()) {
            for (KeeperContainerMeta keeperContainerMeta: o.getKeeperContainers()) {
                KeeperContainerMeta cloneKeeperContainer = MetaCloneFacade.INSTANCE.clone(keeperContainerMeta);
                clone.addKeeperContainer(cloneKeeperContainer);
            }
        }

        if (null != o.getApplierContainers()) {
            for (ApplierContainerMeta applierContainerMeta: o.getApplierContainers()) {
                ApplierContainerMeta cloneApplierContainer = MetaCloneFacade.INSTANCE.clone(applierContainerMeta);
                clone.addApplierContainer(cloneApplierContainer);
            }
        }

        if (null != o.getSentinels()) {
            for (SentinelMeta sentinelMeta: o.getSentinels().values()) {
                SentinelMeta cloneSentinel = MetaCloneFacade.INSTANCE.clone(sentinelMeta);
                clone.addSentinel(cloneSentinel);
            }
        }

        if (null != o.getMetaServers()) {
            for (MetaServerMeta metaServerMeta: o.getMetaServers()) {
                MetaServerMeta cloneMetaServer = MetaCloneFacade.INSTANCE.clone(metaServerMeta);
                clone.addMetaServer(cloneMetaServer);
            }
        }

        if (null != o.getAzs()) {
            for (AzMeta azMeta: o.getAzs()) {
                AzMeta cloneAz = MetaCloneFacade.INSTANCE.clone(azMeta);
                clone.addAz(cloneAz);
            }
        }

        if (null != o.getZkServer()) {
            ZkServerMeta cloneZkServer = MetaCloneFacade.INSTANCE.clone(o.getZkServer());
            cloneZkServer.setParent(clone);
            clone.setZkServer(cloneZkServer);
        }

        return clone;
    }
}
