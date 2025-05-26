package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

public class KeeperGapAllowedSync extends AbstractKeeperIntegratedSingleDc {

	private String INFO_KEY_ROLE = "role";
	private String INFO_KEY_MASTER_REPLID = "master_replid";
	private String INFO_KEY_MASTER_REPLOFF = "master_repl_offset";
	private String INFO_KEY_MASTER_LINK_STATUS = "master_link_status";

	private String INFO_KEY_REPL_MODE = "gtid_repl_mode";
	private String INFO_KEY_GTID_SET = "gtid_set";
	private String INFO_KEY_GTID_EXECUTED = "gtid_executed";
	private String INFO_KEY_UUID = "gtid_uuid";
	private String INFO_KEY_MASTER_UUID = "gtid_master_uuid";

	@Override
	protected void endPrepareRedisConfig(RedisMeta redisMeta, StringBuilder sb) {
		sb.append("gtid-enabled yes\r\n");
	}
	@Override
	protected File getRedisDataDir(RedisMeta redisMeta, File redisDir) {
		return new File(new File(redisDir, "data"), redisMeta.getPort().toString());
	}

	private RedisMeta getSlaveMeta() {
		return getRedisSlaves().get(0);
	}

	protected List<RedisMeta> getDcRedises(String dc, String clusterId, String shardId) {
		List<RedisMeta> redises = new ArrayList<>();
		redises.add(getRedisMaster());
		redises.add(getSlaveMeta());
		return redises;
	}

	private void jedisExecCommand(String host, int port, String method, String... args) {
		try (Jedis jedis = new Jedis(host, port)) {
			if (method.equalsIgnoreCase("SET")) {
				jedis.set(args[0], args[1]);
			} else if (method.equalsIgnoreCase("SLAVEOF")) {
				if ("NO".equalsIgnoreCase(args[0])) {
					jedis.slaveofNoOne();
				} else {
					jedis.slaveof(args[0], Integer.parseInt(args[1]));
				}
			} else if (method.equalsIgnoreCase("GET")) {
				logger.info("[xxxx] GET {} => {}", args[0], jedis.get(args[0]));
			} else {
				throw new IllegalArgumentException("method not supported:" + method);
			}
		}
	}

	@Before
	public void restoreReplicationLinkBeforeTest() throws Exception {
		slaveExecCommand("SLAVEOF", "NO", "ONE");
		sleep(100);
		waitConditionUntilTimeOut(() -> {
					try {
						return Objects.equals(slaveInfoKey(InfoCommand.INFO_TYPE.REPLICATION, INFO_KEY_ROLE), "master");
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}, 10*1000, 100
		);

		logger.info(remarkableMessage("xxxx " + name.getMethodName() + " slave-port:" + getSlaveMeta().getPort() + " xxxxx"));
	}

	private void assertReplStreamAligned() throws Exception {
		// repl_mode
		String masterReplMode = masterInfoKey(InfoCommand.INFO_TYPE.GTID, INFO_KEY_REPL_MODE);
		for (RedisMeta redisMeta : getDcRedises(dc, getClusterId(), getShardId())) {
			String replMode = infoRedis(redisMeta.getIp(), redisMeta.getPort(), InfoCommand.INFO_TYPE.GTID, INFO_KEY_REPL_MODE);
			Assert.assertEquals(masterReplMode, replMode);
		}
		for(KeeperMeta keeperMeta : getDcKeepers(dc, getClusterId(), getShardId())){
			String replMode = getRedisKeeperServer(keeperMeta).getReplicationStore().getMetaStore().getCurrentReplStage().getProto().toString().toLowerCase();
			Assert.assertEquals(masterReplMode, replMode);
		}
		// master_uuid
		String masterMyUuid = masterInfoKey(InfoCommand.INFO_TYPE.GTID, INFO_KEY_UUID);
		String masterMasterUuid = masterInfoKey(InfoCommand.INFO_TYPE.GTID, INFO_KEY_MASTER_UUID);
		Assert.assertEquals(masterMyUuid, masterMasterUuid);
		for (RedisMeta redisMeta : getDcRedises(dc, getClusterId(), getShardId())) {
			String masterUuid = infoRedis(redisMeta.getIp(), redisMeta.getPort(), InfoCommand.INFO_TYPE.GTID, INFO_KEY_MASTER_UUID);
			Assert.assertEquals(masterMyUuid, masterUuid);
		}
		for(KeeperMeta keeperMeta : getDcKeepers(dc, getClusterId(), getShardId())){
			String masterUuid = getRedisKeeperServer(keeperMeta).getReplicationStore().getMetaStore().getCurrentReplStage().getMasterUuid();
			Assert.assertEquals(masterMyUuid, masterUuid);
		}
		//gtid_set
		GtidSet masterGtidSet = new GtidSet(masterInfoKey(InfoCommand.INFO_TYPE.GTID, INFO_KEY_GTID_SET));
		for (RedisMeta redisMeta : getDcRedises(dc, getClusterId(), getShardId())) {
			GtidSet gtidSet = new GtidSet(infoRedis(redisMeta.getIp(), redisMeta.getPort(), InfoCommand.INFO_TYPE.GTID, INFO_KEY_GTID_SET));
			Assert.assertEquals(masterGtidSet, gtidSet);
		}
		for(KeeperMeta keeperMeta : getDcKeepers(dc, getClusterId(), getShardId())){
			Pair<GtidSet, GtidSet> gtidSets = getRedisKeeperServer(keeperMeta).getReplicationStore().getGtidSet();
			GtidSet gtidSet = gtidSets.getKey().union(gtidSets.getValue());
			if (!masterGtidSet.equals(gtidSet)) {
				logger.info("[xxxx] keeperMeta: {}", keeperMeta);
			}
			Assert.assertEquals(masterGtidSet, gtidSet);
		}
		//replid
		String masterMyReplId = masterInfoKey(InfoCommand.INFO_TYPE.REPLICATION, INFO_KEY_MASTER_REPLID);
		for (RedisMeta redisMeta : getDcRedises(dc, getClusterId(), getShardId())) {
			String masterReplId = infoRedis(redisMeta.getIp(), redisMeta.getPort(), InfoCommand.INFO_TYPE.REPLICATION, INFO_KEY_MASTER_REPLID);
			Assert.assertEquals(masterMyReplId, masterReplId);
		}
		for(KeeperMeta keeperMeta : getDcKeepers(dc, getClusterId(), getShardId())){
			String masterReplId = getRedisKeeperServer(keeperMeta).getReplicationStore().getMetaStore().getCurrentReplStage().getReplId();
			Assert.assertEquals(masterMyReplId, masterReplId);
		}
		//reploff
		waitConditionUntilTimeOut(() -> {
					long masterReplOff;
					try {
						masterReplOff = Long.parseLong(masterInfoKey(InfoCommand.INFO_TYPE.REPLICATION, INFO_KEY_MASTER_REPLOFF));
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
					for (RedisMeta redisMeta : getDcRedises(dc, getClusterId(), getShardId())) {
						Long replOff = null;
						try {
							replOff = Long.parseLong(infoRedis(redisMeta.getIp(), redisMeta.getPort(), InfoCommand.INFO_TYPE.REPLICATION, INFO_KEY_MASTER_REPLOFF));
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
						if (masterReplOff != replOff) return false;
					}
					for(KeeperMeta keeperMeta : getDcKeepers(dc, getClusterId(), getShardId())){
						long replOff = getRedisKeeperServer(keeperMeta).getReplicationStore().getCurReplStageReplOff();
						if (masterReplOff != replOff) return false;
					}
					return true;
				}, 1000, 100
		);
	}

	@Test
	public void testGtidNotRelated_XFullResync() throws Exception {

		long fullSyncCount = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();

		masterExecCommand("SET", "hello", "world");
		slaveExecCommand("SET", "foo", "bar");

		String masterGtidExecuted = masterInfoKey(InfoCommand.INFO_TYPE.GTID, INFO_KEY_GTID_EXECUTED);
		String activeKeeperGtidExecuted = infoRedis(activeKeeper.getIp(), activeKeeper.getPort(), InfoCommand.INFO_TYPE.GTID, INFO_KEY_GTID_EXECUTED);
		String backupKeeperGtidExecuted = infoRedis(backupKeeper.getIp(), backupKeeper.getPort(), InfoCommand.INFO_TYPE.GTID, INFO_KEY_GTID_EXECUTED);
		String slaveGtidExecuted = slaveInfoKey(InfoCommand.INFO_TYPE.GTID, INFO_KEY_GTID_EXECUTED);

		Assert.assertEquals(masterGtidExecuted, activeKeeperGtidExecuted);
		Assert.assertEquals(masterGtidExecuted, backupKeeperGtidExecuted);
		Assert.assertNotEquals(masterGtidExecuted, slaveGtidExecuted);

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());

		slaveWaitForSync();

		long fullSyncCount2 = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();
		Assert.assertEquals(fullSyncCount, fullSyncCount2); //TODO for now it is xcontinue, should be xfullresync

		logger.info("[xxxx] waiting replstream align for 10 seconds");

		sleep(10*1000);

		assertReplStreamAligned();
	}

//	@Test
//	public void testReplStageNotFound_SlaveXsync_XFullResync() throws Exception {
//		// master: | (X) set hello world | (P) set hello world_1 | (X) set hello world_2 |
//		// slave:  | (X)
//	}
//
//	@Test
//	public void testReplStageNotFound_SlavePsync_XFullResync() throws Exception {
//		// master: | (P) set hello world_1 set hello world_2 | (X) set hello world_3 | (P) set hello world_4 |
//		// slave:  | (P) set hello world_1 |
//
//	}
//
//	@Test
//	public void testGapExeedsLimit_XFullResync() throws Exception {
//	}
//
//	@Test
//	public void testGapWithinLimit_XContinue() throws Exception {
//	}
//
//	@Test
//	public void testLocateInPrevPsyncStage_ContinueThenXContinue() throws Exception {
//	}
//
//	@Test
//	public void testLocateInPrevXsyncStage_GtidNotMatch_XFullResync() throws Exception {
//	}
//
//	@Test
//	public void testLocateInPrevXsyncStage_GtidMatch_XContinueThenContinue() throws Exception {
//	}

	private void masterExecCommand(String method, String... args) {
		jedisExecCommand(redisMaster.getIp(), redisMaster.getPort(), method, args);
	}

	private String masterInfoKey(InfoCommand.INFO_TYPE section, String key) throws Exception {
		return infoRedis(redisMaster.getIp(), redisMaster.getPort(), section, key);
	}

	private void slaveExecCommand(String method, String... args) {
		jedisExecCommand(getSlaveMeta().getIp(), getSlaveMeta().getPort(), method, args);
	}

	private String slaveInfoKey(InfoCommand.INFO_TYPE section, String key) throws Exception {
		return infoRedis(getSlaveMeta().getIp(), getSlaveMeta().getPort(), section, key);
	}
	private void slaveWaitForSync() throws TimeoutException {
		waitConditionUntilTimeOut(() -> {
			try {
				return Objects.equals(slaveInfoKey(InfoCommand.INFO_TYPE.REPLICATION, INFO_KEY_MASTER_LINK_STATUS), "up");
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}, 10*1000,100);
	}

}
