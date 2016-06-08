package com.ctrip.xpipe.redis.keeper.cluster;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.redis.keeper.entity.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.meta.MetaService;

/**
 * @author marsqing
 *
 *         May 30, 2016 2:39:37 PM
 */
@Component
public class KeeperStarter {

	@Autowired
	private MetaService metaService;

	public void waitUntilActive(String clusterId, String shardId, KeeperMeta keeper) {
		while (true) {
			KeeperMeta activeKeeper = metaService.getActiveKeeper(clusterId, shardId);
			if (activeKeeper != null && activeKeeper.getIp().equals(keeper.getIp())
			      && activeKeeper.getPort().equals(keeper.getPort())) {
				return;
			} else {
				// TODO
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					break;
				}
			}
		}

	}

}
