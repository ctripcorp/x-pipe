package com.ctrip.xpipe.utils.log;

import org.slf4j.MDC;

/**
 * @author marsqing
 *
 *         Dec 20, 2016 1:52:46 PM
 */
public class MDCUtil {

	public static void setClusterShard(String cluster, String shard) {
		MDC.put("xpipe.cluster.shard", "[" + cluster + "." + shard + "]");
	}

	public static Runnable decorateClusterShardMDC(Runnable r, String cluster, String shard) {
		return new Runnable() {

			@Override
			public void run() {
				MDCUtil.setClusterShard(cluster, shard);
				r.run();
			}
		};
	}

}
