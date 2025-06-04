package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
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
	private String INFO_KEY_PREV_REPL_MODE = "gtid_prev_repl_mode";
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

	@Before
	public void restoreReplicationLinkBeforeTest() throws Exception {
		masterExecCommand("CONFIG", "SET", "gtid-enabled", "yes");
		slaveExecCommand("CONFIG", "SET", "gtid-enabled", "yes");
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

		boolean isXsyncMode = Objects.equals(masterReplMode, "xsync");

		if (isXsyncMode) {
			// master_uuid
			String masterMyUuid = masterInfoKey(InfoCommand.INFO_TYPE.GTID, INFO_KEY_UUID);
			String masterMasterUuid = masterInfoKey(InfoCommand.INFO_TYPE.GTID, INFO_KEY_MASTER_UUID);
			Assert.assertEquals(masterMyUuid, masterMasterUuid);
			for (RedisMeta redisMeta : getDcRedises(dc, getClusterId(), getShardId())) {
				String masterUuid = infoRedis(redisMeta.getIp(), redisMeta.getPort(), InfoCommand.INFO_TYPE.GTID, INFO_KEY_MASTER_UUID);
				Assert.assertEquals(masterMyUuid, masterUuid);
			}
			for (KeeperMeta keeperMeta : getDcKeepers(dc, getClusterId(), getShardId())) {
				String masterUuid = getRedisKeeperServer(keeperMeta).getReplicationStore().getMetaStore().getCurrentReplStage().getMasterUuid();
				Assert.assertEquals(masterMyUuid, masterUuid);
			}
			//gtid_set
			GtidSet masterGtidSet = new GtidSet(masterInfoKey(InfoCommand.INFO_TYPE.GTID, INFO_KEY_GTID_SET));
			for (RedisMeta redisMeta : getDcRedises(dc, getClusterId(), getShardId())) {
				GtidSet gtidSet = new GtidSet(infoRedis(redisMeta.getIp(), redisMeta.getPort(), InfoCommand.INFO_TYPE.GTID, INFO_KEY_GTID_SET));
				Assert.assertEquals(masterGtidSet, gtidSet);
			}
			for (KeeperMeta keeperMeta : getDcKeepers(dc, getClusterId(), getShardId())) {
				Pair<GtidSet, GtidSet> gtidSets = getRedisKeeperServer(keeperMeta).getReplicationStore().getGtidSet();
				GtidSet gtidSet = gtidSets.getKey().union(gtidSets.getValue());

				if (!masterGtidSet.equals(gtidSet)) {
					logger.info("[xxxx] masterGtidSet:{}, gtidSet:{}, keeperMeta:{}", masterGtidSet, gtidSet, keeperMeta);
					for (int i = 0; i < 360; i++) {
						sleep(10 * 1000);
						Pair<GtidSet, GtidSet> gtidSets2 = getRedisKeeperServer(keeperMeta).getReplicationStore().getGtidSet();
						GtidSet gtidSet2 = gtidSets2.getKey().union(gtidSets2.getValue());
						logger.info("[xxxx] gtidSet:{}", gtidSet2);
					}
				}
				Assert.assertEquals(masterGtidSet, gtidSet);
			}
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

		waitForSync();

		long fullSyncCount2 = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();
		Assert.assertEquals(fullSyncCount+1, fullSyncCount2);

		assertReplStreamAligned();
	}

	@Test
	public void testReplStageNotFound_SlaveXsync_XFullResync() throws Exception {
		// master: | (X) set hello world | (P) set hello world_1 | (X) set hello world_2 |
		// slave:  | (X)

		Assert.assertEquals(masterInfoKey(InfoCommand.INFO_TYPE.GTID, INFO_KEY_GTID_EXECUTED), slaveInfoKey(InfoCommand.INFO_TYPE.GTID, INFO_KEY_GTID_EXECUTED));

		masterExecCommand("SET", "hello", "world");

		Assert.assertNotEquals(masterInfoKey(InfoCommand.INFO_TYPE.GTID, INFO_KEY_GTID_EXECUTED), slaveInfoKey(InfoCommand.INFO_TYPE.GTID, INFO_KEY_GTID_EXECUTED));

		masterExecCommand("CONFIG", "SET", "gtid-enabled", "no");
		Assert.assertEquals(masterInfoKey(InfoCommand.INFO_TYPE.GTID, INFO_KEY_REPL_MODE), "psync");
		Assert.assertEquals(masterInfoKey(InfoCommand.INFO_TYPE.GTID, INFO_KEY_PREV_REPL_MODE), "xsync");
		masterExecCommand("SET", "hello", "world_1");

		masterExecCommand("CONFIG", "SET", "gtid-enabled", "yes");
		Assert.assertEquals(masterInfoKey(InfoCommand.INFO_TYPE.GTID, INFO_KEY_REPL_MODE), "xsync");
		Assert.assertEquals(masterInfoKey(InfoCommand.INFO_TYPE.GTID, INFO_KEY_PREV_REPL_MODE), "psync");
		masterExecCommand("SET", "hello", "world_2");

		long fullSyncCount = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());

		waitForSync();

		assertReplStreamAligned();

		long fullSyncCount2 = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();
		Assert.assertTrue(fullSyncCount < fullSyncCount2);
	}

	@Test
	public void testReplStageNotFound_SlavePsync_XFullResync() throws Exception {
		// master: | (P) set hello world_1 set hello world_2 | (X) set hello world_3 | (P) set hello world_4 |
		// slave:  | (P) set hello world_1 |

		masterExecCommand("CONFIG", "SET", "gtid-enabled", "no");
		slaveExecCommand("CONFIG", "SET", "gtid-enabled", "no");

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		waitForSync();

		long fullSyncCount = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();

		masterExecCommand("SET", "hello", "world_1");
		waitForSync();

		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world_1");

		slaveExecCommand("SLAVEOF", "127.0.0.1", "0");

		masterExecCommand("SET", "hello", "world_2");

		masterExecCommand("CONFIG", "SET", "gtid-enabled", "yes");
		masterExecCommand("SET", "hello", "world_3");

		masterExecCommand("CONFIG", "SET", "gtid-enabled", "no");
		masterExecCommand("SET", "hello", "world_4");

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		waitForSync();
		assertReplStreamAligned();

		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world_4");

		long fullSyncCount2 = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();
		Assert.assertTrue(fullSyncCount < fullSyncCount2);
	}

	@Test
	public void testGapExeedsLimit_XFullResync() throws Exception {
		long fullSyncCount = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();
		slaveExecCommand("CONFIG", "SET", "gtid-xsync-max-gap", "3");

		for (int i = 0; i < 5; i++) {
			slaveExecCommand("SET", "key_"+i, "value"+i);
		}

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		waitForSync();

		long fullSyncCount2 = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();
		Assert.assertEquals(fullSyncCount+1, fullSyncCount2);

		slaveExecCommand("CONFIG", "SET", "gtid-xsync-max-gap", "10000");
	}

	@Test
	public void testGapWithinLimit_XContinue() throws Exception {
		// write anything so that master-slave gtid related.
		masterExecCommand("SET", "foo", "bar");
		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		waitForSync();

		slaveExecCommand("SLAVEOF", "NO", "ONE");
		sleep(1000);

		long fullSyncCount = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();

		for (int i = 0; i < 5; i++) {
			slaveExecCommand("SET", "key_"+i, "value"+i);
		}

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		waitForSync();

		long fullSyncCount2 = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();
		Assert.assertEquals(fullSyncCount, fullSyncCount2);
	}

	@Test
	public void testLocateInPrevPsyncStage_ContinueThenXContinue() throws Exception {
		// master: | (P) set hello world_1 set hello world_2 | (X) set hello world_3 |
		// slave:  | (P) set hello world_1 |

		masterExecCommand("CONFIG", "SET", "gtid-enabled", "no");
		slaveExecCommand("CONFIG", "SET", "gtid-enabled", "no");

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		waitForSync();

		long fullSyncCount = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();

		masterExecCommand("SET", "hello", "world_1");
		waitForSync();
		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world_1");

		slaveExecCommand("SLAVEOF", "127.0.0.1", "0");

		masterExecCommand("SET", "hello", "world_2");
		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world_1");

		masterExecCommand("CONFIG", "SET", "gtid-enabled", "yes");
		masterExecCommand("SET", "hello", "world_3");

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		waitForSync();
		assertReplStreamAligned();

		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world_3");

		long fullSyncCount2 = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();
		Assert.assertEquals(fullSyncCount, fullSyncCount2);
	}

	@Test
	public void testReplStageNotMatch_XFullResync() throws Exception {
		//master: | (psync) set hello world_1 | (X) set hello world_2 set hello world_3 |
		//slave:  | (psync) set hello world_1  set hello world_2 |

		masterExecCommand("CONFIG", "SET", "gtid-enabled", "no");
		slaveExecCommand("CONFIG", "SET", "gtid-enabled", "no");

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		waitForSync();
		assertReplStreamAligned();

		long fullSyncCount = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();

		masterExecCommand("SET", "hello", "world_1");
		waitForSync();
		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world_1");

		slaveExecCommand("SLAVEOF", "NO", "ONE");
		slaveExecCommand("SET", "hello", "world_2");

		masterExecCommand("CONFIG", "SET", "gtid-enabled", "yes");
		masterExecCommand("SET", "hello", "world_2");
		masterExecCommand("SET", "hello", "world_3");

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		waitForSync();
		assertReplStreamAligned();

		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world_3");

		long fullSyncCount2 = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();
		Assert.assertEquals(fullSyncCount+1, fullSyncCount2);
	}

	@Test
	public void testLocateInPrevXsyncStage_GtidMatch_XContinueThenContinue() throws Exception {
		// master: | (X) set hello world_1 set hello world_2 | (P) set hello world_3 |
		// slave:  | (X) set hello world_1 |

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		waitForSync();

		long fullSyncCount = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();

		masterExecCommand("SET", "hello", "world_1");
		waitForSync();
		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world_1");

		slaveExecCommand("SLAVEOF", "127.0.0.1", "0");

		masterExecCommand("SET", "hello", "world_2");

		masterExecCommand("CONFIG", "SET", "gtid-enabled", "no");
		masterExecCommand("SET", "hello", "world_3");

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		waitForSync();
		assertReplStreamAligned();

		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world_3");

		long fullSyncCount2 = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();
		Assert.assertEquals(fullSyncCount, fullSyncCount2);
	}

	@Test
	public void testLocateInPrevXsyncStage_GapWithinLimit_XContinueThenContinue() throws Exception {
		// master: | (X) set hello world_1 set hello world_3 | (P) set hello world_4 |
		// slave:  | (X) set hello world_1 set hello world_2 |

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		waitForSync();

		long fullSyncCount = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();

		masterExecCommand("SET", "hello", "world_1");
		waitForSync();
		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world_1");
		assertReplStreamAligned();

		slaveExecCommand("SLAVEOF", "NO", "ONE");
		slaveExecCommand("SET", "hello", "world_2");

		masterExecCommand("SET", "hello", "world_3");

		masterExecCommand("CONFIG", "SET", "gtid-enabled", "no");
		masterExecCommand("SET", "hello", "world_4");

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		waitForSync();
		assertReplStreamAligned();

		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world_4");

		long fullSyncCount2 = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();
		Assert.assertEquals(fullSyncCount, fullSyncCount2);
	}

	@Test
	public void testLocateInPrevXsyncStage_GapExceedsLimit_XFullResync() throws Exception {
		// master: | (X) set hello world_0 set foo bar_0 | (P) set hello world_2a |
		// slave : | (X) set hello world_0 ...(10000)... set hello world_10001 set hello world_2b |

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		waitForSync();

		masterExecCommand("SET", "hello", "world_0");
		waitForSync();
		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world_0");
		assertReplStreamAligned();

		slaveExecCommand("SLAVEOF", "NO", "ONE");

		masterExecCommand("SET", "foo", "bar_0");

		masterExecCommand("CONFIG", "SET", "gtid-enabled", "no");
		sleep(2000);

		masterExecCommand("SET", "hello", "world_2a");

		slaveExecCommand("CONFIG", "SET", "gtid-xsync-max-gap", "3");
		for (int i = 0; i < 5; i++) {
			slaveExecCommand("SET", "key_"+i, "value"+i);
		}
		slaveExecCommand("SET", "hello", "world_2b");

		long fullSyncCount = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		waitForSync();
		assertReplStreamAligned();

		Assert.assertEquals(slaveExecCommand("GET", "foo"), "bar_0");
		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world_2a");

		long fullSyncCount2 = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();
		Assert.assertEquals(fullSyncCount+1, fullSyncCount2);

		slaveExecCommand("CONFIG", "SET", "gtid-xsync-max-gap", "10000");
	}

	@Test
	public void testLocatePrevReplStage_StageNotMatch_FullResync() throws Exception {
		// master: | (P) set hello world | (X) set hello world_0             set hello world_1a | (P) set hello world_2 |
		// slave : | (P) set hello world | (X) set hello world_0 | (P) set hello world_1b |

		masterExecCommand("CONFIG", "SET", "gtid-enabled", "no");
		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		masterExecCommand("SET", "hello", "world");
		waitForSync();
		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world");

		masterExecCommand("CONFIG", "SET", "gtid-enabled", "yes");
		masterExecCommand("SET", "hello", "world_0");
		waitForSync();
		assertReplStreamAligned();
		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world_0");

		slaveExecCommand("SLAVEOF", "NO", "ONE");
		slaveExecCommand("CONFIG", "SET", "gtid-enabled", "no");
		slaveExecCommand("SET", "hello", "world_1b");

		masterExecCommand("SET", "hello", "world_1a");

		masterExecCommand("CONFIG", "SET", "gtid-enabled", "no");
		masterExecCommand("SET", "hello", "world_2");

		long fullSyncCount = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		waitForSync();
		assertReplStreamAligned();

		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world_2");

		long fullSyncCount2 = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();
		Assert.assertTrue(fullSyncCount < fullSyncCount2);
	}

	@Test
	public void testPsync_OffsetValid_Psync() throws Exception {
		// master: | (P) set hello world set hello world_1 |
		// slave : | (P) set hello world |

		masterExecCommand("CONFIG", "SET", "gtid-enabled", "no");

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());

		masterExecCommand("SET", "hello", "world");

		waitForSync();
		assertReplStreamAligned();
		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world");

		slaveExecCommand("SLAVEOF", "127.0.0.1", "0");

		masterExecCommand("SET", "hello", "world_1");
		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world");

		long fullSyncCount = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		waitForSync();
		assertReplStreamAligned();

		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world_1");

		long fullSyncCount2 = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();
		Assert.assertEquals(fullSyncCount, fullSyncCount2);
	}

	@Test
	public void testPsync_ReplIdChanged_Fullresync() throws Exception {
		// master: | (P) set hello world set hello world_1 |
		// slave : | (P) set hello world; |

		masterExecCommand("CONFIG", "SET", "gtid-enabled", "no");
		slaveExecCommand("CONFIG", "SET", "gtid-enabled", "no");

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());

		masterExecCommand("SET", "hello", "world");

		waitForSync();
		assertReplStreamAligned();
		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world");

		slaveExecCommand("SLAVEOF", "NO", "ONE");

		masterExecCommand("SET", "hello", "world_1");
		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world");

		long fullSyncCount = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();

		slaveExecCommand("SLAVEOF", activeKeeper.getIp(), activeKeeper.getPort().toString());
		waitForSync();
		assertReplStreamAligned();

		Assert.assertEquals(slaveExecCommand("GET", "hello"), "world_1");

		long fullSyncCount2 = getRedisKeeperServer(activeKeeper).getKeeperMonitor().getKeeperStats().getFullSyncCount();
		Assert.assertEquals(fullSyncCount+1, fullSyncCount2);
	}

	private Object jedisExecCommand(String host, int port, String method, String... args) {
		Object result = null;

		try (Jedis jedis = new Jedis(host, port)) {
			if (method.equalsIgnoreCase("SET")) {
				jedis.set(args[0], args[1]);
			} else if (method.equalsIgnoreCase("SLAVEOF")) {
				if ("NO".equalsIgnoreCase(args[0])) {
					jedis.slaveofNoOne();
				} else {
					jedis.slaveof(args[0], Integer.parseInt(args[1]));
				}
			} else if (method.equalsIgnoreCase("CONFIG")) {
				if ("SET".equalsIgnoreCase(args[0])) {
					jedis.configSet(args[1], args[2]);
				} else {
					result = jedis.configGet(args[1]);
				}
			} else if (method.equalsIgnoreCase("GET")) {
				result = jedis.get(args[0]);
			} else {
				throw new IllegalArgumentException("method not supported:" + method);
			}
		}

		return result;
	}

	private Object masterExecCommand(String method, String... args) {
		return jedisExecCommand(redisMaster.getIp(), redisMaster.getPort(), method, args);
	}

	private String masterInfoKey(InfoCommand.INFO_TYPE section, String key) throws Exception {
		return infoRedis(redisMaster.getIp(), redisMaster.getPort(), section, key);
	}

	private Object slaveExecCommand(String method, String... args) {
		return jedisExecCommand(getSlaveMeta().getIp(), getSlaveMeta().getPort(), method, args);
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

	private void waitForSync() throws Exception {
		sleep(2000);
		waitConditionUntilTimeOut(() -> {
			long masterReplOff;
			try {
				masterReplOff = Long.parseLong(masterInfoKey(InfoCommand.INFO_TYPE.REPLICATION, INFO_KEY_MASTER_REPLOFF));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			for (RedisMeta redisMeta : getDcRedises(dc, getClusterId(), getShardId())) {
				Long replOff = null;
				String masterLinkStatus = null;
				try {
					replOff = Long.parseLong(infoRedis(redisMeta.getIp(), redisMeta.getPort(), InfoCommand.INFO_TYPE.REPLICATION, INFO_KEY_MASTER_REPLOFF));
					masterLinkStatus = infoRedis(redisMeta.getIp(), redisMeta.getPort(), InfoCommand.INFO_TYPE.REPLICATION, INFO_KEY_MASTER_LINK_STATUS);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				if (Objects.equals(masterLinkStatus, "down")) {
					logger.info("[waitForSync] masterLinkStatus down, redis:{}", redisMeta.getPort());
					return false;
				}
				if (masterReplOff != replOff) {
					logger.info("[waitForSync] masterReplOff:{} != replOff:{}, redis:{}", masterReplOff, replOff, redisMeta.getPort());
					return false;
				}
			}
			for(KeeperMeta keeperMeta : getDcKeepers(dc, getClusterId(), getShardId())){
				long replOff;
				try {
					replOff = getRedisKeeperServer(keeperMeta).getReplicationStore().getCurReplStageReplOff();
				} catch (Exception e) {
					logger.info("[waitForSync] exception:{}, keeper:{}",  e.getClass().getSimpleName(), keeperMeta.getPort());
					return false;
				}
				if (getRedisKeeperServer(keeperMeta).getRedisMaster().getMasterState() != MASTER_STATE.REDIS_REPL_CONNECTED) {
					logger.info("[waitForSync] masterLinkStatus down, keeper:{}",  keeperMeta.getPort());
					return false;
				}
				if (masterReplOff != replOff) {
					logger.info("[waitForSync] masterReplOff:{} != replOff:{}, keeper:{}",  masterReplOff, replOff, keeperMeta.getPort());
					return false;
				}
			}

			return true;
		}, 60*1000, 100);
	}

}
