package com.ctrip.xpipe.redis.meta.server.meta;


import java.util.Set;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;

/**
 * @author wenchao.meng
 *
 * Aug 7, 2016
 */
public interface DcMetaCache extends Observable{

	Set<String> getClusters();

	ClusterMeta getClusterMeta(String clusterId);

	KeeperContainerMeta getKeeperContainer(KeeperMeta keeperMeta);
}
