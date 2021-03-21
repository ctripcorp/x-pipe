package com.ctrip.xpipe.utils.log;

import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.MDC;

/**
 * @author marsqing
 *
 *         Dec 20, 2016 1:52:46 PM
 */
public class MDCUtil {

	protected static String MDC_KEY =  "xpipe.cluster.shard";

	public static void setClusterShard(String cluster, String shard) {

		MDC.put(MDC_KEY, StringUtil.makeSimpleName(cluster, shard));
	}

	@VisibleForTesting
	protected static String getClusterShard() {
		return MDC.get(MDC_KEY);

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
