package com.ctrip.xpipe.redis.core.metaserver;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;

import java.util.List;

/**
 * @author wenchao.meng
 *
 *         Aug 2, 2016
 */
public interface MetaServerKeeperService extends MetaServerService {

	public static String PATH_PING = "cluster/{clusterId}/shard/{shardId}/ping";

	public static String PATH_GET_ALL_KEEPERS = "getallkeepers";

	/*********************** for keeper *******************/

	void ping(String clusterId, String shardId, KeeperInstanceMeta keeperInstanceMeta);

	/*********************** for container *******************/

	/**
	 * meta server merge all meta server's results
	 * 
	 * @param keeperContainerMeta
	 * @return
	 */
	List<KeeperTransMeta> getAllKeepersByKeeperContainer(KeeperContainerMeta keeperContainerMeta);

}
