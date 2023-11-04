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

	protected static String MDC_KEY_CLUSTER_SHARD =  "xpipe.cluster.shard";

	protected static String MDC_KEY_KEEPER_REPL = "xpipe.keeper.repl";

	public static void setClusterShard(String cluster, String shard) {

		MDC.put(MDC_KEY_CLUSTER_SHARD, StringUtil.makeSimpleName(cluster, shard));
	}

	public static void setKeeperRepl(String replId) {

		MDC.put(MDC_KEY_KEEPER_REPL, StringUtil.makeSimpleName(replId, null));
	}

	@VisibleForTesting
	protected static String getClusterShard() {
		return MDC.get(MDC_KEY_CLUSTER_SHARD);

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

	public static Runnable decorateKeeperReplMDC(Runnable r, String replId) {
		return new Runnable() {

			@Override
			public void run() {
				MDCUtil.setKeeperRepl(replId);
				r.run();
			}
		};
	}

}
