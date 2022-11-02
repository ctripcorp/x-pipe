package com.ctrip.xpipe.redis.console.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author shyin
 *
 * Sep 8, 2016
 */
public class ShardModel implements java.io.Serializable{
	private static final long serialVersionUID = 1L;
	
	private ShardTbl shardTbl;
	
	private List<RedisTbl> m_keepers = new ArrayList<RedisTbl>();
	private List<RedisTbl> m_redises = new ArrayList<RedisTbl>();
	private List<ApplierTbl> m_appliers = new ArrayList<ApplierTbl>();
	
	/** for creation **/
	private Map<Long, SentinelGroupModel> sentinels;
	
	public ShardModel(){
	}

	public ShardModel(List<RedisTbl> m_redises){
		this.m_redises = m_redises;
	}


	public ShardModel addKeeper(RedisTbl keeper) {
		m_keepers.add(keeper);
		return this;
	}
	
	public ShardModel addRedis(RedisTbl redis) {
		m_redises.add(redis);
		return this;
	}

	public ShardModel addApplier(ApplierTbl applier) {
		m_appliers.add(applier);
		return this;
	}

	public ShardModel setKeepers(List<RedisTbl> keepers) {
		this.m_keepers = keepers;
		return this;
	}

	public List<ApplierTbl> getAppliers() {
		return m_appliers;
	}
	
	public List<RedisTbl> getKeepers() {
		return m_keepers;
	}
	
	public List<RedisTbl> getRedises() {
		return m_redises;
	}
	
	public ShardTbl getShardTbl() {
		return this.shardTbl;
	}
	
	public void setShardTbl(ShardTbl shardTbl) {
		this.shardTbl = shardTbl;
	}
	
	public Map<Long, SentinelGroupModel> getSentinels() {
		return this.sentinels;
	}
	
	public void setSentinels(Map<Long, SentinelGroupModel> sentinels) {
		this.sentinels = sentinels;
	}

	@Override
	public String toString() {
		return String.format("shard:%s, keepers:%s, redises:%s, appliers:%s", shardTbl, m_keepers, m_redises, m_appliers);
	}
}
