package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.KeeperMasterChooser;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 *         Nov 4, 2016
 */
public abstract class AbstractKeeperMasterChooser extends AbstractStartStoppable implements KeeperMasterChooser {

	protected Logger logger = LoggerFactory.getLogger(getClass());

	public static int DEFAULT_KEEPER_MASTER_CHECK_INTERVAL_SECONDS = Integer
			.parseInt(System.getProperty("KEEPER_MASTER_CHECK_INTERVAL_SECONDS", "5"));

	protected DcMetaCache dcMetaCache;

	protected CurrentMetaManager currentMetaManager;

	protected ScheduledExecutorService scheduled;

	private ScheduledFuture<?> future;

	protected String clusterId, shardId;

	protected int checkIntervalSeconds;

	public AbstractKeeperMasterChooser(String clusterId, String shardId, DcMetaCache dcMetaCache,
			CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled) {
		this(clusterId, shardId, dcMetaCache, currentMetaManager, scheduled,
				DEFAULT_KEEPER_MASTER_CHECK_INTERVAL_SECONDS);
	}

	public AbstractKeeperMasterChooser(String clusterId, String shardId, DcMetaCache dcMetaCache,
			CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled, int checkIntervalSeconds) {

		this.dcMetaCache = dcMetaCache;
		this.currentMetaManager = currentMetaManager;
		this.scheduled = scheduled;
		this.clusterId = clusterId;
		this.shardId = shardId;
		this.checkIntervalSeconds = checkIntervalSeconds;
	}

	@Override
	protected void doStart() throws Exception {
		
		future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

			@Override
			protected void doRun() throws Exception {

				Pair<String, Integer> keeperMaster = chooseKeeperMaster();
				logger.debug("[doRun]{}, {}, {}", clusterId, shardId, keeperMaster);
				Pair<String, Integer> currentMaster = currentMetaManager.getKeeperMaster(clusterId, shardId);
				if (keeperMaster == null || keeperMaster.equals(currentMaster)) {
					logger.debug("[doRun][new master null or equals old master]{}", keeperMaster);
					return;
				}
				logger.debug("[doRun][set]{}, {}, {}", clusterId, shardId, keeperMaster);
				currentMetaManager.setKeeperMaster(clusterId, shardId, keeperMaster.getKey(), keeperMaster.getValue());
			}


		}, 0, checkIntervalSeconds, TimeUnit.SECONDS);
	}

	@Override
	protected void doStop() throws Exception {

		if (future != null) {
			logger.info("[doStop]");
			future.cancel(true);
		}

	}

	protected abstract Pair<String, Integer> chooseKeeperMaster();

	@Override
	public void release() throws Exception {
		stop();
	}

	//for test
	protected ScheduledFuture<?> getFuture() {
		return future;
	}
	
	@Override
	public String toString() {
		return String.format("%s,%s,%s", getClass().getSimpleName(), clusterId, shardId);
	}
}
