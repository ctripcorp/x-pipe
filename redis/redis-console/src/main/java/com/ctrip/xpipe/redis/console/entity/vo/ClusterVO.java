package com.ctrip.xpipe.redis.console.entity.vo;

import java.util.List;

import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;

public class ClusterVO {

	private ClusterTbl baseInfo;
	private List<DC> dcs;

	public ClusterTbl getBaseInfo() {
		return baseInfo;
	}

	public void setBaseInfo(ClusterTbl baseInfo) {
		this.baseInfo = baseInfo;
	}

	public List<DC> getDcs() {
		return dcs;
	}

	public void setDcs(List<DC> dcs) {
		this.dcs = dcs;
	}

	public static class DC {

		private DcTbl baseInfo;
		private List<Shard> shards;

		public DcTbl getBaseInfo() {
			return baseInfo;
		}

		public void setBaseInfo(DcTbl baseInfo) {
			this.baseInfo = baseInfo;
		}

		public List<Shard> getShards() {
			return shards;
		}

		public void setShards(List<Shard> shards) {
			this.shards = shards;
		}
	}

	public static class Shard {

		private ShardTbl baseInfo;

		private List<Redis> redises;

		public ShardTbl getBaseInfo() {
			return baseInfo;
		}

		public void setBaseInfo(ShardTbl baseInfo) {
			this.baseInfo = baseInfo;
		}

		public List<Redis> getRedises() {
			return redises;
		}

		public void setRedises(List<Redis> redises) {
			this.redises = redises;
		}
	}

	public static class Redis {

		private String id;
		private String ip;
		private int port;
		private boolean isActive;
		private RedisRole role;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getIp() {
			return ip;
		}

		public void setIp(String ip) {
			this.ip = ip;
		}

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		public boolean isActive() {
			return isActive;
		}

		public void setActive(boolean active) {
			isActive = active;
		}

		public RedisRole getRole() {
			return role;
		}

		public void setRole(RedisRole role) {
			this.role = role;
		}
	}

	public enum RedisRole {
		MASTER, SLAVE, KEEPER
	}

}
