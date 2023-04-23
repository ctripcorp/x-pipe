package com.ctrip.xpipe.redis.console.model;

import java.util.ArrayList;
import java.util.List;

public class SourceModel implements java.io.Serializable{

	private static final long serialVersionUID = 1L;

	private List<ShardModel> shards = new ArrayList<>();

	private ReplDirectionInfoModel replDirectionInfoModel;

	public SourceModel(){
	}

	public List<ShardModel> getShards() {
		return shards;
	}

	public void setShards(List<ShardModel> shards) {
		this.shards = shards;
	}

	public SourceModel addShardModel(ShardModel shardModel) {
		shards.add(shardModel);
		return this;
	}

	public ReplDirectionInfoModel getReplDirectionInfoModel() {
		return replDirectionInfoModel;
	}

	public SourceModel setReplDirectionInfoModel(ReplDirectionInfoModel replDirectionInfoModel) {
		this.replDirectionInfoModel = replDirectionInfoModel;
		return this;
	}

	@Override
	public String toString() {
		return "SourceModel{" +
				"shardModels=" + shards +
				", replDirectionInfoModel=" + replDirectionInfoModel +
				'}';
	}
}
