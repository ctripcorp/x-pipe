package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

public class GapAllowedReplicationStoreTest extends AbstractRedisKeeperTest{

	private String replidA = "000000000000000000000000000000000000000A";
	private String replidB = "000000000000000000000000000000000000000B";
	private String replidC = "000000000000000000000000000000000000000C";
	private String masterUuidA = "111111111111111111111111111111111111111A";
	private String masterUuidB = "111111111111111111111111111111111111111B";
	private String masterUuidC = "111111111111111111111111111111111111111C";
	private File baseDir;
	private GtidReplicationStore store;
	private RedisOpParser redisOpParser;

	private byte[] generateVanillaCommands(int contentLen) {
		return randomString(contentLen).getBytes();
	}

	private byte[] generateGtidCommands(String uuid, long startGno, int cmdCount) throws IOException {
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		for (int i = 0; i < cmdCount; i++) {
			String uuidGno = uuid + ":" + String.valueOf(startGno+i);
			os.write("*6\r\n".getBytes());
			os.write("$4\r\nGTID\r\n".getBytes());
			os.write('$'); os.write(String.valueOf(uuidGno.length()).getBytes()); os.write("\r\n".getBytes()); os.write(uuidGno.getBytes()); os.write("\r\n".getBytes());
			os.write("$1\r\n0\r\n".getBytes());
			os.write("$3\r\nSET\r\n".getBytes());
			os.write("$3\r\nFOO\r\n".getBytes());
			os.write("$3\r\nBAR\r\n".getBytes());
		}
		return os.toByteArray();
	}

	@Before
	public void beforeDefaultReplicationStoreTest() throws IOException{
		RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
		RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
		redisOpParser = new GeneralRedisOpParser(redisOpParserManager);
		baseDir = new File(getTestFileDir());
		store = new GtidReplicationStore(baseDir, new DefaultKeeperConfig(), randomKeeperRunid(), createkeeperMonitor(), redisOpParser, Mockito.mock(SyncRateManager.class));
	}

	@Test
	public void testGetCurReplStageReplOff() throws IOException {
		RdbStore rdbStore = store.prepareRdb(replidA, 0, new LenEofType(100), ReplStage.ReplProto.XSYNC, new GtidSet(GtidSet.EMPTY_GTIDSET), masterUuidA);
		rdbStore.updateRdbType(RdbStore.Type.NORMAL);
		rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
		store.confirmRdbGapAllowed(rdbStore);
		Assert.assertEquals(store.getCurReplStageReplOff(), 0);
		store.appendCommands(Unpooled.wrappedBuffer(generateGtidCommands(masterUuidA,1,100)));
		Assert.assertTrue(store.getCurReplStageReplOff() >  0);

		store.switchToPSync(replidB, 2000);
		Assert.assertEquals(store.getCurReplStageReplOff(), 2000);
		store.appendCommands(Unpooled.wrappedBuffer(generateVanillaCommands(1000)));
		Assert.assertEquals(store.getCurReplStageReplOff(), 3000);
	}

	@Test
	public void testGetGtidSet() throws IOException {
		Pair<GtidSet,GtidSet> gtidSets;

		RdbStore rdbStore = store.prepareRdb(replidA, 10000, new LenEofType(100), ReplStage.ReplProto.XSYNC, new GtidSet(masterUuidC + ":1-100"), masterUuidA);
		rdbStore.updateRdbType(RdbStore.Type.NORMAL);
		rdbStore.updateRdbGtidSet(masterUuidA + ":1-100,"+ masterUuidB + ":1-100");
		store.confirmRdbGapAllowed(rdbStore);

		gtidSets = store.getGtidSet();
		Assert.assertEquals(gtidSets.getKey(), new GtidSet(masterUuidA+ ":1-100," + masterUuidB+ ":1-100"));
		Assert.assertEquals(gtidSets.getValue(), new GtidSet(masterUuidC + ":1-100"));

		store.appendCommands(Unpooled.wrappedBuffer(generateGtidCommands(masterUuidA,101, 100)));

		gtidSets = store.getGtidSet();
		Assert.assertEquals(gtidSets.getKey(), new GtidSet(masterUuidA + ":1-200," + masterUuidB + ":1-100"));
		Assert.assertEquals(gtidSets.getValue(), new GtidSet(masterUuidC + ":1-100"));

		store.switchToPSync(replidB, 20000);

		gtidSets = store.getGtidSet();
		Assert.assertEquals(gtidSets.getKey(), new GtidSet(masterUuidA + ":1-200," + masterUuidB + ":1-100"));
		Assert.assertEquals(gtidSets.getValue(), new GtidSet(masterUuidC + ":1-100"));

		store.switchToXSync(replidC, 30000, masterUuidC, new GtidSet(masterUuidA + ":1-100," + masterUuidB + ":1-100," + masterUuidC + ":1-100"), new GtidSet(GtidSet.EMPTY_GTIDSET));
		gtidSets = store.getGtidSet();
		Assert.assertEquals(gtidSets.getKey(), new GtidSet(masterUuidA + ":1-100," + masterUuidB + ":1-100," + masterUuidC + ":1-100"));
		Assert.assertEquals(gtidSets.getValue(), new GtidSet(GtidSet.EMPTY_GTIDSET));

		store.appendCommands(Unpooled.wrappedBuffer(generateGtidCommands(masterUuidB,101, 100)));
		gtidSets = store.getGtidSet();
		Assert.assertEquals(gtidSets.getKey(), new GtidSet(masterUuidA + ":1-100," + masterUuidB + ":1-200," + masterUuidC + ":1-100"));
		Assert.assertEquals(gtidSets.getValue(), new GtidSet(GtidSet.EMPTY_GTIDSET));
	}

	private DumpedRdbStore prepareNewRdbXsync() throws IOException {
		DumpedRdbStore dumpedRdbStore = store.prepareNewRdb();
		dumpedRdbStore.updateRdbType(RdbStore.Type.NORMAL);
		dumpedRdbStore.setReplId(replidA);
		dumpedRdbStore.setEofType(new LenEofType(100));
		dumpedRdbStore.setRdbOffset(20000);
		dumpedRdbStore.setReplProto(ReplStage.ReplProto.XSYNC);
		dumpedRdbStore.setMasterUuid(masterUuidA);
		return dumpedRdbStore;
	}
	@Test
	public void testCheckReplIdAndUpdateRdbGapAllowed_Xsync() throws Exception {
		DumpedRdbStore dumpedRdbStore;

		RdbStore rdbStore = store.prepareRdb(replidA, 10000, new LenEofType(100), ReplStage.ReplProto.XSYNC, new GtidSet(masterUuidC + ":1-100"), masterUuidA);
		rdbStore.updateRdbType(RdbStore.Type.NORMAL);
		rdbStore.updateRdbGtidSet(masterUuidA + ":1-100,"+ masterUuidB + ":1-100");
		store.confirmRdbGapAllowed(rdbStore);
		store.appendCommands(Unpooled.wrappedBuffer(generateGtidCommands(masterUuidA,101, 50)));
		long expectedBacklogOffset1 = store.backlogEndOffset();
		store.appendCommands(Unpooled.wrappedBuffer(generateGtidCommands(masterUuidA,151, 50)));
		long expectedBacklogOffset2 = store.backlogEndOffset();

		dumpedRdbStore = prepareNewRdbXsync();
		dumpedRdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
		Assert.assertEquals(store.checkReplIdAndUpdateRdbGapAllowed(dumpedRdbStore), UPDATE_RDB_RESULT.GTID_SET_NOT_MATCH);
		dumpedRdbStore.close();

		dumpedRdbStore = prepareNewRdbXsync();
		dumpedRdbStore.updateRdbGtidSet(masterUuidA + ":1-201," + masterUuidB + ":1-100");
		dumpedRdbStore.setGtidLost(masterUuidC + ":1-100");
		Assert.assertEquals(store.checkReplIdAndUpdateRdbGapAllowed(dumpedRdbStore), UPDATE_RDB_RESULT.RDB_MORE_RECENT);
		dumpedRdbStore.close();

		dumpedRdbStore = prepareNewRdbXsync();
		dumpedRdbStore.updateRdbGtidSet(masterUuidA + ":1-150," + masterUuidB + ":1-100");
		dumpedRdbStore.setGtidLost(masterUuidC + ":1-100");
		Assert.assertEquals(store.checkReplIdAndUpdateRdbGapAllowed(dumpedRdbStore), UPDATE_RDB_RESULT.OK);
		ReplStage replStage = store.getMetaStore().getCurrentReplStage();
		Assert.assertEquals(replStage.getBeginGtidset(), new GtidSet(masterUuidA + ":1-100,"+ masterUuidB + ":1-100"));
		Assert.assertEquals(replStage.getGtidLost(), new GtidSet(masterUuidC + ":1-100"));
		ReplicationStoreMeta metaDup = store.getMetaStore().dupReplicationStoreMeta();
		Assert.assertEquals(metaDup.getRdbGtidSet(), masterUuidA + ":1-150," + masterUuidB + ":1-100");
		Assert.assertEquals((long)metaDup.getRdbContiguousBacklogOffset(), expectedBacklogOffset1);
		dumpedRdbStore.close();

		dumpedRdbStore = prepareNewRdbXsync();
		dumpedRdbStore.updateRdbGtidSet(masterUuidA + ":1-200," + masterUuidB + ":1-100");
		dumpedRdbStore.setGtidLost(masterUuidC + ":1-100");
		Assert.assertEquals(store.checkReplIdAndUpdateRdbGapAllowed(dumpedRdbStore), UPDATE_RDB_RESULT.OK);
		replStage = store.getMetaStore().getCurrentReplStage();
		Assert.assertEquals(replStage.getBeginGtidset(), new GtidSet(masterUuidA + ":1-100,"+ masterUuidB + ":1-100"));
		Assert.assertEquals(replStage.getGtidLost(), new GtidSet(masterUuidC + ":1-100"));
		metaDup = store.getMetaStore().dupReplicationStoreMeta();
		Assert.assertEquals(metaDup.getRdbGtidSet(), masterUuidA + ":1-200," + masterUuidB + ":1-100");
		Assert.assertEquals((long)metaDup.getRdbContiguousBacklogOffset(), expectedBacklogOffset2);
		dumpedRdbStore.close();
	}

	private DumpedRdbStore prepareNewRdbPsync() throws IOException {
		DumpedRdbStore dumpedRdbStore = store.prepareNewRdb();
		dumpedRdbStore.updateRdbType(RdbStore.Type.NORMAL);
		dumpedRdbStore.setReplId(replidA);
		dumpedRdbStore.setEofType(new LenEofType(100));
		dumpedRdbStore.setRdbOffset(20000);
		dumpedRdbStore.setReplProto(ReplStage.ReplProto.PSYNC);
		dumpedRdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
		return dumpedRdbStore;
	}

	@Test
	public void testCheckReplIdAndUpdateRdbGapAllowed_Psync() throws Exception {
		DumpedRdbStore dumpedRdbStore;

		RdbStore rdbStore = store.prepareRdb(replidA, 10000, new LenEofType(100), ReplStage.ReplProto.PSYNC, null, masterUuidA);
		rdbStore.updateRdbType(RdbStore.Type.NORMAL);
		rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
		store.confirmRdbGapAllowed(rdbStore);
		store.appendCommands(Unpooled.wrappedBuffer(generateVanillaCommands(1000)));

		dumpedRdbStore = prepareNewRdbXsync();
		dumpedRdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
		Assert.assertEquals(store.checkReplIdAndUpdateRdbGapAllowed(dumpedRdbStore), UPDATE_RDB_RESULT.REPLSTAGE_NOT_MATCH);
		dumpedRdbStore.close();

		dumpedRdbStore = prepareNewRdbPsync();
		dumpedRdbStore.setRdbOffset(5000);
		Assert.assertEquals(store.checkReplIdAndUpdateRdbGapAllowed(dumpedRdbStore), UPDATE_RDB_RESULT.REPLOFF_OUT_RANGE);
		dumpedRdbStore.close();

		dumpedRdbStore = prepareNewRdbPsync();
		dumpedRdbStore.setReplId(replidB);
		Assert.assertEquals(store.checkReplIdAndUpdateRdbGapAllowed(dumpedRdbStore), UPDATE_RDB_RESULT.REPLID_NOT_MATCH);
		dumpedRdbStore.close();

		dumpedRdbStore = prepareNewRdbPsync();
		dumpedRdbStore.setRdbOffset(10500);
		Assert.assertEquals(store.checkReplIdAndUpdateRdbGapAllowed(dumpedRdbStore), UPDATE_RDB_RESULT.OK);
		dumpedRdbStore.close();

		dumpedRdbStore = prepareNewRdbPsync();
		dumpedRdbStore.setRdbOffset(11000);
		Assert.assertEquals(store.checkReplIdAndUpdateRdbGapAllowed(dumpedRdbStore), UPDATE_RDB_RESULT.OK);
		dumpedRdbStore.close();

		dumpedRdbStore = prepareNewRdbPsync();
		dumpedRdbStore.setRdbOffset(12000);
		Assert.assertEquals(store.checkReplIdAndUpdateRdbGapAllowed(dumpedRdbStore), UPDATE_RDB_RESULT.RDB_MORE_RECENT);
		dumpedRdbStore.close();
	}

	@Test
	public void testReXContinue() throws IOException {
		RdbStore rdbStore = store.prepareRdb(replidA, 10000, new LenEofType(100), ReplStage.ReplProto.XSYNC, new GtidSet(masterUuidC + ":1-100"), masterUuidA);
		rdbStore.updateRdbType(RdbStore.Type.NORMAL);
		rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
		store.confirmRdbGapAllowed(rdbStore);
		Pair<GtidSet, GtidSet> gtidSet = store.getGtidSet();

		byte[] wholeCommandBytes = generateGtidCommands(masterUuidA, 101, 100);
		// wholeCommandBytes拆成两个bytes
		byte[] part1 = new byte[20];
		System.arraycopy(wholeCommandBytes, 0, part1, 0, 20);
		byte[] part2 = new byte[wholeCommandBytes.length - 20];
		System.arraycopy(wholeCommandBytes, 20, part2, 0, wholeCommandBytes.length - 20);
		// now just write some bytes
		store.appendCommands(Unpooled.wrappedBuffer(part1));
		Pair<GtidSet, GtidSet> gtidSet2 = store.getGtidSet();
		Assert.assertEquals(gtidSet2, gtidSet);
		// disconnected
		store.resetStateForContinue();
		// continue
		store.appendCommands(Unpooled.wrappedBuffer(part1));
		store.appendCommands(Unpooled.wrappedBuffer(part2));
		gtidSet = store.getGtidSet();
		Assert.assertEquals(gtidSet.getKey(), new GtidSet(masterUuidA + ":101-200"));
		Assert.assertEquals(gtidSet.getValue(), new GtidSet(masterUuidC + ":1-100"));
	}

}
