package com.ctrip.xpipe.redis.console.service.notifier;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author shyin
 *
 *         Sep 6, 2016
 */
@Component
public class DefaultClusterMetaModifiedNotifier implements ClusterMetaModifiedNotifier {
	private Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private ConsoleConfig config;
	@Autowired
	private ClusterMetaService clusterMetaService;
	@Autowired
	private MetaServerConsoleServiceManagerWrapper metaServerConsoleServiceManagerWrapper;

	private ExecutorService fixedThreadPool;

	@PostConstruct
	public void postConstruct() {
		fixedThreadPool = Executors.newFixedThreadPool(config.getConsoleNotifyThreads(),
				XpipeThreadFactory.create("ConsoleNotifierThreadPool"));
	}

	@Override
	public void notifyClusterUpdate(final String dcName, final String clusterName) {
		submitNotifyTask(new MetaNotifyTask<Void>("notifyClusterUpdate", config.getConsoleNotifyRetryTimes(),
				new MetaNotifyRetryPolicy(config.getConsoleNotifyRetryInterval())) {

			@Override
			public Void doNotify() {
				logger.info("[notifyClusterUpdate]{},{}", dcName, clusterName);
				metaServerConsoleServiceManagerWrapper.get(dcName).clusterModified(clusterName,
						clusterMetaService.getClusterMeta(dcName, clusterName));
				return null;
			}
		});
	}

	@Override
	public void notifyClusterDelete(final String clusterName, List<DcTbl> dcs) {
		if (null != dcs) {
			for (final DcTbl dc : dcs) {
				submitNotifyTask(new MetaNotifyTask<Void>("notifyClusterDelete", config.getConsoleNotifyRetryTimes(),
						new MetaNotifyRetryPolicy(config.getConsoleNotifyRetryInterval())) {

					@Override
					public Void doNotify() {
						logger.info("[notifyClusterDelete]{},{}", clusterName, dc.getDcName());
						metaServerConsoleServiceManagerWrapper.get(dc.getDcName()).clusterDeleted(clusterName);
						return null;
					}
				});
			}
		}
	}

	@Override
	public void notifyUpstreamChanged(final String clusterName, final String shardName, final String ip, final int port,
			List<DcTbl> dcs) {
		if (null != dcs) {
			for (final DcTbl dc : dcs) {
				submitNotifyTask(new MetaNotifyTask<Void>("notifyUpstreamChanged", config.getConsoleNotifyRetryTimes(),
						new MetaNotifyRetryPolicy(config.getConsoleNotifyRetryInterval())) {

					@Override
					public Void doNotify() {
						logger.info("[notifyUpstreamChanged]{},{},{},{},{}", clusterName, shardName, ip, port,
								dc.getDcName());
						metaServerConsoleServiceManagerWrapper.get(dc.getDcName()).upstreamChange(clusterName,
								shardName, ip, port);
						return null;
					}
				});
			}
		}
	}

	@SuppressWarnings("rawtypes")
	protected void submitNotifyTask(MetaNotifyTask task) {
		fixedThreadPool.submit(task);
	}
}
