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

	public void setActiveKeeper(KeeperMeta activeKeeper) {
		this.activeKeeper = activeKeeper;
	}

	public void setUpstreamKeeper(KeeperMeta upstreamKeeper) {
		this.upstreamKeeper = upstreamKeeper;
	}

	public void setRedisMaster(RedisMeta redisMaster) {
		this.redisMaster = redisMaster;
	}
	
	@Override
	public boolean equals(Object obj) {
		
		if(!(obj instanceof ShardStatus)){
			return false;
		}
		ShardStatus cmp = (ShardStatus) obj;
		
		if(!equals(this.activeKeeper, cmp.activeKeeper)){
			return false;
		}

		if(!equals(this.redisMaster, cmp.redisMaster)){
			return false;
		}

		if(!equals(this.upstreamKeeper, cmp.upstreamKeeper)){
			return false;
		}

		return true;
	}

	private boolean equals(Object obj1, Object obj2) {
		
		if(obj1 == obj2){
			return true;
		}
		
		if(obj1 == null || obj2 == null){
			return false;
		}
		
		return obj1.equals(obj2);
	}

}
