package com.ctrip.xpipe.redis.keeper.meta;

import java.util.concurrent.atomic.AtomicReference;

import com.ctrip.xpipe.redis.keeper.entity.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.entity.RedisMeta;

/**
 * @author marsqing
 *
 *         Jun 13, 2016 2:05:38 PM
 */
public class ShardStatus {

	private AtomicReference<KeeperMeta> activeKeeper = new AtomicReference<>();

	private AtomicReference<KeeperMeta> upstreamKeeper = new AtomicReference<>();

	private AtomicReference<RedisMeta> redisMaster = new AtomicReference<>();

	public ShardStatus() {
	}

	public ShardStatus(KeeperMeta activeKeeper, KeeperMeta upstreamKeeper, RedisMeta redisMaster) {
		this.activeKeeper.set(activeKeeper);
		this.upstreamKeeper.set(upstreamKeeper);
		this.redisMaster.set(redisMaster);
	}

	public KeeperMeta getActiveKeeper() {
		return activeKeeper.get();
	}

	public KeeperMeta getUpstreamKeeper() {
		return upstreamKeeper.get();
	}

	public RedisMeta getRedisMaster() {
		return redisMaster.get();
	}

	public void setActiveKeeper(KeeperMeta activeKeeper) {
		this.activeKeeper.set(activeKeeper);
	}

	public void setUpstreamKeeper(KeeperMeta upstreamKeeper) {
		this.upstreamKeeper.set(upstreamKeeper);
	}

	public void setRedisMaster(RedisMeta redisMaster) {
		this.redisMaster.set(redisMaster);
	}

}
