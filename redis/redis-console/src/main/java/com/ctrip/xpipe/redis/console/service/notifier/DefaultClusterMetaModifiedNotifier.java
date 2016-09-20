package com.ctrip.xpipe.redis.console.service.notifier;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;

/**
 * @author shyin
 *
 *         Sep 6, 2016
 */
@Component
public class DefaultClusterMetaModifiedNotifier implements ClusterMetaModifiedNotifier {
	Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private ClusterMetaService clusterMetaService;
	@Autowired
	private MetaServerConsoleServiceManagerWrapper metaServerConsoleServiceManagerWrapper;

	@Override
	public void notifyClusterUpdate(final String dcName, final String clusterName) {
		logger.info("[notifyClusterUpdate][construct]{},{}", dcName, clusterName);
		metaServerConsoleServiceManagerWrapper.get(dcName).clusterModified(clusterName,
				clusterMetaService.getClusterMeta(dcName, clusterName));
		logger.info("[notifyClusterUpdate][finish]{},{}", dcName, clusterName);

	}

	@Override
	public void notifyClusterDelete(final String clusterName, List<DcTbl> dcs) {
		if (null != dcs) {
			for (DcTbl dc : dcs) {
				logger.info("[notifyClusterDelete][construct]{},{}", clusterName, dc.getDcName());
				metaServerConsoleServiceManagerWrapper.get(dc.getDcName()).clusterDeleted(clusterName);
				logger.info("[notifyClusterDelete][finish]{},{}", clusterName, dc.getDcName());
			}
		}
	}

	@Override
	public void notifyUpstreamChanged(String clusterName, String shardName, String ip, int port, List<DcTbl> dcs) {
		if (null != dcs) {
			for (DcTbl dc : dcs) {
				logger.info("[notifyUpstreamChanged][construct]{},{},{},{},{}", clusterName, shardName, ip, port,
						dc.getDcName());
				metaServerConsoleServiceManagerWrapper.get(dc.getDcName()).upstreamChange(clusterName, shardName, ip,
						port);
				logger.info("[notifyUpstreamChanged][finish]{},{},{},{},{}", clusterName, shardName, ip, port,
						dc.getDcName());
			}
		}

	}
}
