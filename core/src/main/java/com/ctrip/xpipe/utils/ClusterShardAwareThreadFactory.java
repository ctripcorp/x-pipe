package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.utils.log.MDCUtil;

import java.util.concurrent.ThreadFactory;

/**
 * @author marsqing
 *
 *         Dec 20, 2016 1:47:35 PM
 */
public final class ClusterShardAwareThreadFactory extends XpipeThreadFactory {

	private String cluster;
	private String shard;

	public static ThreadFactory create(String cluster, String shard, String namePrefix, boolean daemon) {
		return new ClusterShardAwareThreadFactory(cluster, shard, namePrefix, daemon);
	}

	public static ThreadFactory create(String cluster, String shard, String namePrefix) {
		return create(cluster, shard, namePrefix, false);
	}

	public static ThreadFactory create(Object cluster, Object shard, String namePrefix) {
		String clusterString = null;
		String shardString = null;
		if (cluster != null) {
			clusterString = cluster.toString();
		}
		if (shard != null) {
			shardString = shard.toString();
		}
		return create(clusterString, shardString, namePrefix, false);
	}

	private ClusterShardAwareThreadFactory(String cluster, String shard, String namePrefix, boolean daemon) {
		super(namePrefix, daemon);
		this.cluster = cluster;
		this.shard = shard;
	}

	private ClusterShardAwareThreadFactory(String cluster, String shard, String namePrefix) {
		this(cluster, shard, namePrefix, false);
	}

	@Override
	public Thread newThread(Runnable r) {
		return super.newThread(MDCUtil.decorateClusterShardMDC(r, cluster, shard));
	}

}
