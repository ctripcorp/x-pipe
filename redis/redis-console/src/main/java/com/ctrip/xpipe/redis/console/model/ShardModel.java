package com.ctrip.xpipe.redis.console.model;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shyin
 *
 * Sep 8, 2016
 */
public class ShardModel implements java.io.Serializable{
	private static final long serialVersionUID = 1L;
	
	private String m_id;
	private String m_upstream;
	
	private List<RedisTbl> m_keepers = new ArrayList<RedisTbl>();
	private List<RedisTbl> m_redises = new ArrayList<RedisTbl>();
	
	public ShardModel(){
	}
	
	public ShardModel(String id) {
		m_id = id;
	}
	
	public ShardModel addKeeper(RedisTbl keeper) {
		m_keepers.add(keeper);
		return this;
	}
	
	public ShardModel addRedis(RedisTbl redis) {
		m_redises.add(redis);
		return this;
	}
	
	public String getId() {
		return m_id;
	}
	
	public List<RedisTbl> getKeepers() {
		return m_keepers;
	}
	
	public List<RedisTbl> getRedises() {
		return m_redises;
	}
	
	public String getUpstream() {
		return m_upstream;
	}
	
	public ShardModel setId(String id) {
		m_id = id;
		return this;
	}
	
	public ShardModel setUpstream(String upstream) {
		m_upstream = upstream;
		return this;
	}
}
