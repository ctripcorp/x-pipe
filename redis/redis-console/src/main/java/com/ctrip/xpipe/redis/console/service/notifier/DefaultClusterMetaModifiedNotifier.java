package com.ctrip.xpipe.redis.console.service.notifier;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.constant.XpipeConsoleConstant;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.retry.RetryNTimes;
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

	@Override
	public void notifyClusterUpdate(final String dcName, final String clusterName) {
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(config.getConsoleNotifyThreads(),
				XpipeThreadFactory.create("UpdateNotifierThreadPool"));
		fixedThreadPool.submit(new Runnable() {
			@Override
			public void run() {
				try {
					logger.info("[notifyClusterUpdate][construct]{},{}", dcName, clusterName);
					new RetryNTimes<>(config.getConsoleNotifyRetryTimes(),
							new MetaNotifyRetryPolicy(config.getConsoleNotifyRetryInterval()))
									.execute(new AbstractCommand<Object>() {
										@Override
										public String getName() {
											return "retryable-cluster-update-notifier";
										}

										@Override
										protected void doExecute() throws Exception {
											metaServerConsoleServiceManagerWrapper.get(dcName).clusterModified(
													clusterName,
													clusterMetaService.getClusterMeta(dcName, clusterName));
										}

										@Override
										protected void doReset() {

										}

									});
					logger.info("[notifyClusterUpdate][success]{},{}", dcName, clusterName);
				} catch (Exception e) {
					logger.error("[notifyClusterUpdate][failed]{},{}", dcName, clusterName);
					logger.error("[notifyClusterUpdate][failed][rootCause]{}", e);
				}
			}
		});
	}

	@Override
	public void notifyClusterDelete(final String clusterName, List<DcTbl> dcs) {
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(config.getConsoleNotifyThreads(),
				XpipeThreadFactory.create("DeleteNotifierThreadPool"));
		if (null != dcs) {
			for (final DcTbl dc : dcs) {
				fixedThreadPool.submit(new Runnable() {
					@Override
					public void run() {
						try {
							logger.info("[notifyClusterDelete][construct]{},{}", clusterName, dc.getDcName());
							new RetryNTimes<>(config.getConsoleNotifyRetryTimes(),
									new MetaNotifyRetryPolicy(config.getConsoleNotifyRetryInterval()))
											.execute(new AbstractCommand<Object>() {
												@Override
												public String getName() {
													return "retryable-cluster-delete-notifier";
												}

												@Override
												protected void doExecute() throws Exception {
													metaServerConsoleServiceManagerWrapper.get(dc.getDcName())
															.clusterDeleted(clusterName);
												}

												@Override
												protected void doReset() {
												}

											});
							logger.info("[notifyClusterDelete][success]{},{}", clusterName, dc.getDcName());
						} catch (Exception e) {
							logger.error("[notifyClusterDelete][failed]{},{}", dc.getDcName(), clusterName);
							logger.error("[notifyClusterDelete][failed][rootCause]{}", e);
						}
					}
				});
			}
		}
	}

	@Override
	public void notifyUpstreamChanged(final String clusterName, final String shardName, final String ip, final int port,
			List<DcTbl> dcs) {
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(config.getConsoleNotifyThreads(),
				XpipeThreadFactory.create("UpstreamChangedNotifierThreadPool"));
		if (null != dcs) {
			for (final DcTbl dc : dcs) {
				fixedThreadPool.submit(new Runnable() {
					@Override
					public void run() {
						try {
							logger.info("[notifyUpstreamChanged][construct]{},{},{},{},{}", clusterName, shardName, ip,
									port, dc.getDcName());
							if (!ip.equals(XpipeConsoleConstant.DEFAULT_IP)) {
								new RetryNTimes<>(config.getConsoleNotifyRetryTimes(),
										new MetaNotifyRetryPolicy(config.getConsoleNotifyRetryInterval()))
												.execute(new AbstractCommand<Object>() {

													@Override
													public String getName() {
														return "retryable-upstream-changed-notifier";
													}

													@Override
													protected void doExecute() throws Exception {
														metaServerConsoleServiceManagerWrapper.get(dc.getDcName())
																.upstreamChange(clusterName, shardName, ip, port);
													}

													@Override
													protected void doReset() {
													}

												});

							} else {
								logger.info("[notifyUpstreamChanged][ignored]Ignore with defalut ip : {}",
										XpipeConsoleConstant.DEFAULT_IP);
							}
							logger.info("[notifyUpstreamChanged][success]{},{},{},{},{}", clusterName, shardName, ip,
									port, dc.getDcName());
						} catch (Exception e) {
							logger.error("[notifyUpstreamChanged][failed]{},{},{},{},{}", clusterName, shardName, ip,
									port, dc.getDcName());
							logger.error("[notifyUpstreamChanged][failed][rootCause]{}", e);
						}
					}
				});
			}
		}

	}
}
