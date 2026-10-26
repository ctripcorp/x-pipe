package com.ctrip.xpipe.redis.keeper;


import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.protocal.GapAllowedSyncObserver;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.core.store.XSyncContinue;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.exception.RedisSlavePromotionException;
import com.ctrip.xpipe.redis.keeper.prepare.PrepareWatchSnapshot;
import com.ctrip.xpipe.redis.keeper.pubsub.KeeperPubSubRegistry;
import com.ctrip.xpipe.redis.keeper.impl.SetRdbDumperException;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.store.ck.CKStore;
import com.ctrip.xpipe.redis.keeper.store.searcher.CmdKeyItem;
import com.ctrip.xpipe.redis.keeper.store.searcher.GtidCommandSearcher;
import io.netty.channel.Channel;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午3:09:23
 */
public interface RedisKeeperServer extends RedisServer, GapAllowedSyncObserver, Destroyable{
	
	int getListeningPort();

	/**
	 * {@link KeeperRepl} over {@link #getReplicationStore()} (get semantics); {@code null} when no store is opened.
	 */
	KeeperRepl getKeeperRepl();

	/**
	 * {@link KeeperRepl} over {@link #getOrCreateReplicationStore()}. Data sync and state transition paths only
	 * (m6 spec §9).
	 */
	KeeperRepl getOrCreateKeeperRepl();

	GtidCommandSearcher createCmdKeySearcher(String uuid, long begGno, long endGno);

	XSyncContinue locateContinueGtidSet(GtidSet gtidSet) throws Exception;

	XSyncContinue locateContinueGtidSetWithFallbackToEnd(GtidSet gtidSet) throws Exception;

	XSyncContinue locateTailOfCmd();

	void switchToPSync(String replId, long replOff) throws IOException;

	void switchToXSync(String replId, long replOff, String masterUuid, GtidSet gtidCont, GtidSet gtidLost) throws IOException;

	boolean increaseLost(GtidSet lost, RedisSlave from) throws IOException;

	void clientDisconnected(Channel channel);
	
	String getKeeperRunid();
	
	/**
	 * include all client roles
	 * @return
	 */
	Set<RedisClient> allClients();
	
	Set<RedisSlave> slaves();
		
	/**
	 * get semantics (m6 spec §9.2): the already-opened store if it is {@code checkOk()}, otherwise {@code null}.
	 * Never reopens / creates a store, never does FS IO, never throws for PREPARE. Queries and ops entries
	 * use this; data sync and state transition use {@link #getOrCreateReplicationStore()}.
	 */
	ReplicationStore getReplicationStore();

	/** Error message for get-semantics entries that need a store while none is opened (m6 spec §9.3). */
	String ERR_REPLICATION_STORE_NOT_OPENED = "replication store not opened";

	/**
	 * Current store, reopening {@code latest.store.dir} or creating a new store when none is open
	 * ({@code Manager.createIfNotExist()}). Throws in PREPARE unless the manager is read-only.
	 * Data sync and state transition paths only; queries must not construct a store (m6 spec §9).
	 */
	ReplicationStore getOrCreateReplicationStore();

	/**
	 * Whether the store manager is in PREPARE read-only watch mode (D12).
	 * Default false so existing stubs stay closed-gate.
	 */
	default boolean isReadOnlyStore() {
		return false;
	}

	/**
	 * Container-level TFS mode (m1 D33). Default false so existing stubs stay local-disk.
	 */
	default boolean isTfsMode() {
		return false;
	}

	/**
	 * Already-opened store pointer. No lock, no FS; {@code null} if none is open. Unlike
	 * {@link #getReplicationStore()} it does not filter {@code checkOk()} (PREPARE INFO / ROLE, D10 / D11).
	 */
	default ReplicationStore getOpenedStore() {
		return null;
	}

	/**
	 * Watcher periodic snapshot. Command thread read-only; {@code null} if watch is off or store not opened (D11).
	 */
	default PrepareWatchSnapshot getPrepareWatchSnapshot() {
		return null;
	}

	/**
	 * Per-keeper Pub/Sub registry and async deliverer (D16 / D18). Null when the server
	 * does not host Pub/Sub (existing stubs / mocks).
	 */
	default KeeperPubSubRegistry getPubSubRegistry() {
		return null;
	}

	CKStore getCkStore();

	ReplId getReplId();

	boolean compareAndDo(RedisKeeperServerState expected, Runnable action);
	
	void setRedisKeeperServerState(RedisKeeperServerState redisKeeperServerState);
	
	RedisKeeperServerState getRedisKeeperServerState();

	boolean gapAllowSyncEnabled();
	
	KeeperMeta getCurrentKeeperMeta();
	
	void reconnectMaster();

	/**
	 * Drop an expired store so Backup {@code PartialOnlyGapAllowedSync} sends {@code PSYNC ? -2}.
	 * Called when entering Backup, before {@link #reconnectMaster()}. Not called from initialize
	 * (role is unset; must not {@code getCurrent}/{@code create} for a write lease).
	 */
	void resetReplAfterLongTimeDown();
	
	void stopAndDisposeMaster();
	
	RedisMaster getRedisMaster();
	
	void promoteSlave(String ip, int port) throws RedisSlavePromotionException;

	void closeSlaves(String reason);

	/**
	 * Orchestrate PREPARE transition: stop write → setState PREPARE (reject new slave) →
	 * closeSlaves → {@code replicationStoreManager.stop()} (cancel GC / flush / close handles)
	 * (spec §3.8).
	 * Failures must propagate so Handler returns Redis ERROR (ForceCloseDir).
	 */
	void doBecomePrepare(Endpoint masterAddress);

	/**
	 * PREPARE → ACTIVE/BACKUP re-entry (spec §3.8.3 / T-R.9):
	 * {@code Manager.start()} → {@code createIfNotExist()} (reopen {@code latest.store.dir}) →
	 * Backup also {@link #resetReplAfterLongTimeDown()} →
	 * {@code MetaStore.becomeActive/becomeBackup} (same as {@code doBecomeActive}) → setState → {@code reconnectMaster}.
	 * Must <b>not</b> call {@code create()} when latest store dir already exists, except Backup expiry reset.
	 * Must <b>not</b> call {@code initReplicationStore} (that is NodeAdded / new-store only).
	 */
	void doReenterFromPrepare(Endpoint masterAddress, boolean becomeActive);
	
	public static enum PROMOTION_STATE{
		
		NORMAL,
		BEGIN_PROMOTE_SLAVE,//promote slave to master. 1.should not receive commands, 2. disconnect with master
		SLAVE_PROMTED,
		REPLICATION_META_EXCHANGED
		
	}

	default void fullSyncToSlave(RedisSlave redisSlave) throws IOException {
		// getKeeperMonitor().getKeeperStats().increaseFullSync();
		fullSyncToSlave(redisSlave, false);
	}

	void fullSyncToSlave(RedisSlave redisSlave, boolean freshRdbNeeded) throws IOException;
	
	KeeperInstanceMeta getKeeperInstanceMeta();
	
	KeeperConfig getKeeperConfig();

	void clearRdbDumper(RdbDumper rdbDumper, boolean forceRdb);
	
	void setRdbDumper(RdbDumper rdbDumper) throws SetRdbDumperException;

	void setRdbDumper(RdbDumper rdbDumper, boolean force) throws SetRdbDumperException;
	
	RdbDumper rdbDumper();
	
	KeeperMonitor getKeeperMonitor();

	void resetDefaultReplication();

	PsyncObserver createPsyncObserverForRdbOnlyRepl();

	void resetElection();

	boolean isLeader();

	long getLastElectionResetTime();

	void releaseRdb() throws IOException;

	/**
	 * Close the replication store without deleting it. Unlike {@link #destroy()}, safe to call when
	 * {@code baseDir} is a shared TFS store that must outlive this keeper instance.
	 */
	void release() throws Exception;

	/** Container-level store manager, for callers that must verify liveness before a destructive op. */
	ReplicationStoreManager getReplicationStoreManager();

}
