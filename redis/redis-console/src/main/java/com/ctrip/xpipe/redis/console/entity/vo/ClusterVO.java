package com.ctrip.xpipe.redis.console.entity.vo;

import com.ctrip.xpipe.redis.console.web.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.web.model.DcTbl;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

import java.util.List;

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

	static class DC {

		private DcTbl dcId;
		private List<Shard> shards;

		public DcTbl getDcId() {
			return dcId;
		}

		public void setDcId(DcTbl dcId) {
			this.dcId = dcId;
		}

		public List<Shard> getShards() {
			return shards;
		}

		public void setShards(List<Shard> shards) {
			this.shards = shards;
		}
	}

	static class Shard {

		private List<RedisMeta> redises;

		public List<RedisMeta> getRedises() {
			return redises;
		}

		public void setRedises(List<RedisMeta> redises) {
			this.redises = redises;
		}
	}


}
