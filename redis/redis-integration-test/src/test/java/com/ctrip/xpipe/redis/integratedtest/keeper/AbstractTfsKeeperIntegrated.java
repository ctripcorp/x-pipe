package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractKeeperCommand.KeeperSetStateCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigGetCommand;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.container.ContainerResourceManager;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.monitor.impl.NoneKeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.ratelimit.impl.UnlimitedSyncRateManager;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * TFS 共用目录集成测基类（spec D38 / D39 / §4.12.2）。
 * 不改 {@code AbstractIntegratedTest} 默认工厂（分端口目录 + {@code tfsMode=false}）。
 */
public abstract class AbstractTfsKeeperIntegrated extends AbstractKeeperIntegratedSingleDc {

	protected static final String TFS_STORE_DIR_NAME = "tfs_store";

	/**
	 * 两相节奏的两个阈值压到最小（D48）。tick 固定 1s 不可配，所以一个完整 cycle 仍 ≈2s；
	 * 集成测只验证「两相节奏不破坏既有闭环」，不做「Xs 后可见」的墙钟断言（T-HA.4）。
	 */
	protected static final int PREPARE_WATCH_REOPEN_INTERVAL_MILLI = 50;

	protected static final int PREPARE_WATCH_CLOSE_HOLD_MILLI = 10;

	protected static final int READY_WAIT_MILLI = 30000;

	protected static final int READY_POLL_MILLI = 100;

	protected static final int CONFIG_GET_TIMEOUT_SECONDS = 30;

	private static final int TFS_SET_STATE_TIMEOUT_MILLI = 10000;

	protected TfsComparatorHarness comparatorHarness;

	/**
	 * JVM 级共用一份生产 TailCache（默认 1MiB chunk）。不在 {@code @After}
	 * shutdown：与 keeper dispose / gc 赛跑会把后续用例的 {@code ioExecutor}
	 * 打成 terminated（见 {@code AbstractRedisKeeperTest}）。
	 */
	private static volatile AsyncFileSystem sharedTfsIntegratedFileSystem;

	@Override
	protected void setKeeperState(KeeperMeta keeperMeta, KeeperState keeperState, String ip, Integer port)
			throws Exception {
		KeeperSetStateCommand command = new KeeperSetStateCommand(keeperMeta, keeperState,
				new Pair<>(ip, port), scheduled);
		command.setCommandTimeoutMilli(TFS_SET_STATE_TIMEOUT_MILLI);
		command.execute().sync();
	}

	@After
	public void stopTfsComparator() {
		stopComparator();
	}

	protected File tfsStoreDir() {
		return new File(getTestFileDir(), TFS_STORE_DIR_NAME);
	}

	/**
	 * 失配用例使用独立目录；默认共享目录行为保持不变（D38 / D40）。
	 */
	protected boolean useDivergentStoreDirs() {
		return false;
	}

	/**
	 * 默认两 Keeper 同一 {@code keeperBaseDir}；失配用例按端口拆成两套目录（D40）。
	 */
	protected File keeperBaseDir(KeeperMeta keeperMeta) {
		if (!useDivergentStoreDirs()) {
			return tfsStoreDir();
		}
		return new File(getTestFileDir(), TFS_STORE_DIR_NAME + "_" + keeperMeta.getPort());
	}

	/**
	 * 生产 TailCache 默认 1MiB chunk。单测 {@code createTestAsyncFileSystem} 用 1KiB
	 * 练多 chunk：{@code readAllBytes} 一次 {@code fs.read}，{@code meta.v2.json}
	 * 超过 1KiB 会短读，PREPARE {@code PSYNC ? -4} 打不开店。
	 */
	protected AsyncFileSystem createTfsIntegratedFileSystem(KeeperConfig keeperConfig) {
		return sharedTfsIntegratedFileSystem(keeperConfig);
	}

	static AsyncFileSystem sharedTfsIntegratedFileSystem(KeeperConfig keeperConfig) {
		AsyncFileSystem fs = sharedTfsIntegratedFileSystem;
		if (fs != null) {
			return fs;
		}
		synchronized (AbstractTfsKeeperIntegrated.class) {
			if (sharedTfsIntegratedFileSystem == null) {
				AsyncFileSystem created = ContainerResourceManager.createAsyncFileSystem(keeperConfig);
				Runtime.getRuntime().addShutdownHook(new Thread(() -> {
					try {
						created.shutdown();
					} catch (Throwable t) {
						LoggerFactory.getLogger(AbstractTfsKeeperIntegrated.class)
								.error("[tfs-it-async-fs-shutdown]", t);
					}
				}, "tfs-it-async-fs-shutdown"));
				sharedTfsIntegratedFileSystem = created;
			}
			return sharedTfsIntegratedFileSystem;
		}
	}

	protected File expectedManagerBaseDir() {
		return new File(tfsStoreDir(), ReplId.from(getShardDbId()).toString());
	}

	@Override
	protected KeeperConfig getKeeperConfig() {
		TestKeeperConfig config = (TestKeeperConfig) super.getKeeperConfig();
		config.setPrepareStoreWatchEnabled(true);
		config.setPrepareWatchReopenIntervalMilli(PREPARE_WATCH_REOPEN_INTERVAL_MILLI);
		config.setPrepareWatchCloseHoldMilli(PREPARE_WATCH_CLOSE_HOLD_MILLI);
		return config;
	}

	@Override
	protected void startKeepers() throws Exception {
		FileUtils.forceMkdir(tfsStoreDir());
		super.startKeepers();
	}

	@Override
	protected RedisKeeperServer startKeeper(KeeperMeta keeperMeta, KeeperConfig keeperConfig,
			LeaderElectorManager leaderElectorManager) throws Exception {
		logger.info(remarkableMessage("[startKeeper][tfs]{}, {}"), keeperMeta, keeperConfig);
		File baseDir = keeperBaseDir(keeperMeta);
		RedisKeeperServer redisKeeperServer = createRedisKeeperServer(keeperMeta, baseDir, keeperConfig,
				leaderElectorManager, new NoneKeepersMonitorManager(), new UnlimitedSyncRateManager());
		add(redisKeeperServer);
		return redisKeeperServer;
	}

	@Override
	protected RedisKeeperServer createRedisKeeperServer(KeeperMeta keeperMeta, File baseDir, KeeperConfig keeperConfig,
			LeaderElectorManager leaderElectorManager, KeepersMonitorManager keeperMonitorManager,
			SyncRateManager syncRateManager) {
		Long replId = keeperMeta.parent().getDbId();
		return new DefaultRedisKeeperServer(replId, keeperMeta, keeperConfig, baseDir,
				leaderElectorManager, keeperMonitorManager, resourceManager, syncRateManager, generateRedisOpParser(),
				createTfsIntegratedFileSystem(keeperConfig), null, true);
	}

	@Override
	protected void startRedises() throws ExecuteException, IOException {
		RedisMeta master = getRedisMaster();
		if (master == null) {
			throw new IllegalStateException("no redis master in meta");
		}
		startRedis(master);
	}

	@Override
	protected List<RedisMeta> getRedisSlaves() {
		return Collections.emptyList();
	}

	@Override
	protected int getInitSleepMilli() {
		return 0;
	}

	/**
	 * 父类 {@code @Before} 仍调用此方法；这里只转给 TFS 拓扑，不走 {@code KeeperStateChangeJob}。
	 */
	@Override
	protected final void makeKeeperRight() throws Exception {
		makeTfsKeeperRight();
	}

	protected void makeTfsKeeperRight() throws Exception {
		RedisMeta master = getRedisMaster();
		KeeperMeta active = getKeeperActive();
		List<KeeperMeta> backups = getKeepersBackup();
		if (backups.isEmpty()) {
			throw new IllegalStateException("TFS topology needs Active + Prepare keeper");
		}
		KeeperMeta prepare = backups.get(0);

		logger.info(remarkableMessage("[makeTfsKeeperRight][ACTIVE]{} -> {}:{}"),
				active, master.getIp(), master.getPort());
		setKeeperState(active, KeeperState.ACTIVE, master.getIp(), master.getPort());
		waitTfsActiveReady(active);

		logger.info(remarkableMessage("[makeTfsKeeperRight][PREPARE]{}"), prepare);
		setKeeperState(prepare, KeeperState.PREPARE, master.getIp(), master.getPort());
		waitTfsPrepareReady(prepare);
	}

	protected void waitTfsActiveReady(KeeperMeta active) throws Exception {
		try {
			waitConditionUntilTimeOut(() -> isTfsActiveReady(active), READY_WAIT_MILLI, READY_POLL_MILLI);
		} catch (TimeoutException e) {
			TimeoutException dump = new TimeoutException(describeTfsActiveWait(active));
			dump.initCause(e);
			throw dump;
		}
	}

	protected void waitTfsPrepareReady(KeeperMeta prepare) throws Exception {
		try {
			waitConditionUntilTimeOut(() -> isTfsPrepareReady(prepare), READY_WAIT_MILLI, READY_POLL_MILLI);
		} catch (TimeoutException e) {
			TimeoutException dump = new TimeoutException(describeTfsPrepareWait(prepare));
			dump.initCause(e);
			throw dump;
		}
	}

	private boolean isTfsActiveReady(KeeperMeta active) {
		try {
			RedisKeeperServer server = getRedisKeeperServer(active);
			if (server == null || server.getRedisMaster() == null) {
				return false;
			}
			if (server.getRedisMaster().getMasterState() != MASTER_STATE.REDIS_REPL_CONNECTED) {
				return false;
			}
			ReplicationStore store = server.getReplicationStore();
			return store != null && store.checkOk();
		} catch (Exception e) {
			logger.warn("[waitTfsActiveReady]{}", tfsLogKey(active), e);
			return false;
		}
	}

	private boolean isTfsPrepareReady(KeeperMeta prepare) {
		try {
			RedisKeeperServer server = getRedisKeeperServer(prepare);
			return server != null
					&& server.getRedisKeeperServerState() != null
					&& server.getRedisKeeperServerState().keeperState() == KeeperState.PREPARE
					&& server.isReadOnlyStore();
		} catch (Exception e) {
			logger.warn("[waitTfsPrepareReady]{}", tfsLogKey(prepare), e);
			return false;
		}
	}

	protected String describeTfsActiveWait(KeeperMeta active) {
		try {
			RedisKeeperServer server = getRedisKeeperServer(active);
			if (server == null) {
				return String.format("[waitTfsActiveReady]%s server=null", tfsLogKey(active));
			}
			String masterState = server.getRedisMaster() == null
					? "null" : String.valueOf(server.getRedisMaster().getMasterState());
			ReplicationStore store = server.getReplicationStore();
			return String.format("[waitTfsActiveReady]%s masterState=%s store=%s checkOk=%s",
					tfsLogKey(active), masterState,
					store == null ? "null" : "open",
					store == null ? "null" : store.checkOk());
		} catch (Exception e) {
			return String.format("[waitTfsActiveReady]%s dumpFailed=%s", tfsLogKey(active), e);
		}
	}

	protected String describeTfsPrepareWait(KeeperMeta prepare) {
		try {
			RedisKeeperServer server = getRedisKeeperServer(prepare);
			if (server == null) {
				return String.format("[waitTfsPrepareReady]%s server=null", tfsLogKey(prepare));
			}
			KeeperState state = server.getRedisKeeperServerState() == null
					? null : server.getRedisKeeperServerState().keeperState();
			return String.format("[waitTfsPrepareReady]%s keeperState=%s readOnlyStore=%s",
					tfsLogKey(prepare), state, server.isReadOnlyStore());
		} catch (Exception e) {
			return String.format("[waitTfsPrepareReady]%s dumpFailed=%s", tfsLogKey(prepare), e);
		}
	}

	protected String tfsLogKey(KeeperMeta keeperMeta) {
		return String.format("%s/%s %s %s:%s",
				getClusterId(), getShardId(),
				ReplId.from(getShardDbId()),
				keeperMeta.getIp(), keeperMeta.getPort());
	}

	protected DefaultRedisKeeperServer tfsKeeperServer(KeeperMeta keeperMeta) {
		return (DefaultRedisKeeperServer) getRedisKeeperServer(keeperMeta);
	}

	protected ReplicationStoreManager tfsStoreManager(KeeperMeta keeperMeta) {
		return tfsKeeperServer(keeperMeta).getReplicationStoreManager();
	}

	protected boolean configGetPrepareWatch(KeeperMeta keeperMeta) throws Exception {
		SimpleObjectPool<NettyClient> pool = getXpipeNettyClientKeyedObjectPool()
				.getKeyPool(new DefaultEndPoint(keeperMeta.getIp(), keeperMeta.getPort()));
		return new ConfigGetCommand.ConfigGetPrepareWatch(pool, scheduled)
				.execute().get(CONFIG_GET_TIMEOUT_SECONDS, TimeUnit.SECONDS);
	}

	protected Endpoint keeperEndpoint(KeeperMeta keeperMeta) {
		String ip = StringUtil.isEmpty(keeperMeta.getIp()) ? "localhost" : keeperMeta.getIp();
		return new DefaultEndPoint(ip, keeperMeta.getPort());
	}

	protected TfsComparatorHarness startComparator() {
		return startComparator(CompareReporter.NOOP);
	}

	protected TfsComparatorHarness startComparator(CompareReporter reporter) {
		if (comparatorHarness != null) {
			throw new IllegalStateException("comparator already started");
		}
		if (activeKeeper == null || backupKeeper == null) {
			throw new IllegalStateException("need Active + Prepare keeper");
		}
		comparatorHarness = new TfsComparatorHarness(getClusterId(), getShardId(), getShardDbId(),
				keeperEndpoint(activeKeeper), keeperEndpoint(backupKeeper), scheduled, reporter,
				new ComparatorConfig());
		return comparatorHarness;
	}

	protected void stopComparator() {
		TfsComparatorHarness harness = comparatorHarness;
		comparatorHarness = null;
		if (harness != null) {
			harness.stop();
		}
	}

	protected void waitAligned() throws Exception {
		try {
			waitConditionUntilTimeOut(() -> comparatorHarness != null
							&& comparatorHarness.comparator().isCompareOffsetAligned(),
					READY_WAIT_MILLI, READY_POLL_MILLI);
		} catch (TimeoutException e) {
			TimeoutException dump = new TimeoutException(describeComparatorWait("waitAligned"));
			dump.initCause(e);
			throw dump;
		}
	}

	protected void waitComparedBytesGrow(long from) throws Exception {
		waitComparedBytesGrow(from, READY_WAIT_MILLI);
	}

	protected void waitComparedBytesGrow(long from, int timeoutMilli) throws Exception {
		try {
			waitConditionUntilTimeOut(() -> comparatorHarness != null
							&& comparatorHarness.comparedBytes() > from,
					timeoutMilli, READY_POLL_MILLI);
		} catch (TimeoutException e) {
			TimeoutException dump = new TimeoutException(
					describeComparatorWait("waitComparedBytesGrow from=" + from));
			dump.initCause(e);
			throw dump;
		}
	}

	protected String describeComparatorWait(String prefix) {
		String harnessDump = comparatorHarness == null ? "harness=null" : comparatorHarness.describe();
		return String.format("[%s] %s | %s | %s", prefix, harnessDump,
				describeTfsActiveWait(activeKeeper), describeTfsPrepareWait(backupKeeper));
	}

}
