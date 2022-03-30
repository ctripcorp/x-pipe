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
		return String.format("shard:%s, keepers:%s, redises:%s", shardTbl, m_keepers, m_redises);
	}
}
