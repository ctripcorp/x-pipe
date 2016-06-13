/**
 * 
 */
package com.ctrip.xpipe.redis.keeper.meta;

import com.ctrip.xpipe.redis.keeper.entity.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.entity.RedisMeta;

/**
 * @author marsqing
 *
 *         Jun 13, 2016 2:05:38 PM
 */
public class ShardStatus {

	private KeeperMeta activeKeeper;

	private KeeperMeta upstreamKeeper;

	private RedisMeta redisMaster;

	public ShardStatus() {
	}

	public ShardStatus(KeeperMeta activeKeeper, KeeperMeta upstreamKeeper, RedisMeta redisMaster) {
		this.activeKeeper = activeKeeper;
		this.upstreamKeeper = upstreamKeeper;
		this.redisMaster = redisMaster;
	}

	public KeeperMeta getActiveKeeper() {
		return activeKeeper;
	}

	public KeeperMeta getUpstreamKeeper() {
		return upstreamKeeper;
	}

	public RedisMeta getRedisMaster() {
		return redisMaster;
	}

}
