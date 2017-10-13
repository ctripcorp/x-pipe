package com.ctrip.xpipe.redis.console.notifier;

import java.util.List;

import com.ctrip.xpipe.redis.console.model.DcTbl;

/**
 * @author shyin
 *
 * Sep 14, 2016
 */
public interface ClusterMetaModifiedNotifier {
	public void notifyClusterUpdate(final String dcName, final String clusterName);
	
	public void notifyClusterDelete(final String clusterName, List<DcTbl> dcs);
	
}
