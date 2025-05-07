package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

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
		store.appendCommands(Unpooled.wrappedBuffer(generateVanillaCommands(1000)));
		Assert.assertEquals(store.getCurReplStageReplOff(), 1000);

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

		// TODO uncomment when indexStore ready
		// store.switchToXSync(replidC, 30000, masterUuidC, new GtidSet(masterUuidA + ":1-100," + masterUuidB + ":1-100," + masterUuidC + ":1-100"));
		// gtidSets = store.getGtidSet();
		// Assert.assertEquals(gtidSets.getKey(), new GtidSet(masterUuidA + ":1-100," + masterUuidB + ":1-100," + masterUuidC + ":1-100"));
		// Assert.assertEquals(gtidSets.getValue(), new GtidSet(GtidSet.EMPTY_GTIDSET));

		// store.appendCommands(Unpooled.wrappedBuffer(generateGtidCommands(masterUuidB,101, 100)));
		// gtidSets = store.getGtidSet();
		// Assert.assertEquals(gtidSets.getKey(), new GtidSet(masterUuidA + ":1-100," + masterUuidB + ":1-200," + masterUuidC + ":1-100"));
		// Assert.assertEquals(gtidSets.getValue(), new GtidSet(GtidSet.EMPTY_GTIDSET));
	}

	private DumpedRdbStore prepareNewRdb() throws IOException {
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
	public void testCheckReplIdAndUpdateRdbGapAllowed() throws Exception {
		DumpedRdbStore dumpedRdbStore;

		store = Mockito.spy(store);

		RdbStore rdbStore = store.prepareRdb(replidA, 10000, new LenEofType(100), ReplStage.ReplProto.XSYNC, new GtidSet(masterUuidC + ":1-100"), masterUuidA);
		rdbStore.updateRdbType(RdbStore.Type.NORMAL);
		rdbStore.updateRdbGtidSet(masterUuidA + ":1-100,"+ masterUuidB + ":1-100");
		store.confirmRdbGapAllowed(rdbStore);
		store.appendCommands(Unpooled.wrappedBuffer(generateGtidCommands(masterUuidA,101, 100)));

		dumpedRdbStore = prepareNewRdb();
		dumpedRdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
		//TODO remove when commandstore ready
		doReturn(new XSyncContinue(new GtidSet(GtidSet.EMPTY_GTIDSET),0)).when(store).locateContinueGtidSet(any());
		Assert.assertEquals(store.checkReplIdAndUpdateRdbGapAllowed(dumpedRdbStore), UPDATE_RDB_RESULT.GTID_SET_NOT_MATCH);
		dumpedRdbStore.close();

		dumpedRdbStore = prepareNewRdb();
		dumpedRdbStore.updateRdbGtidSet(masterUuidA + ":1-201," + masterUuidB + ":1-100");
		dumpedRdbStore.setGtidLost(masterUuidC + ":1-100");
		//TODO remove when commandstore ready
		doReturn(new XSyncContinue(new GtidSet(masterUuidA + ":101-200"),store.backlogEndOffset())).when(store).locateContinueGtidSet(any());
		Assert.assertEquals(store.checkReplIdAndUpdateRdbGapAllowed(dumpedRdbStore), UPDATE_RDB_RESULT.RDB_MORE_RECENT);
		dumpedRdbStore.close();

		dumpedRdbStore = prepareNewRdb();
		dumpedRdbStore.updateRdbGtidSet(masterUuidA + ":1-150," + masterUuidB + ":1-100");
		dumpedRdbStore.setGtidLost(masterUuidC + ":1-100");
		//TODO remove when commandstore ready
		doReturn(new XSyncContinue(new GtidSet(masterUuidA + ":101-150"),store.backlogEndOffset() - 1000)).when(store).locateContinueGtidSet(any());
		Assert.assertEquals(store.checkReplIdAndUpdateRdbGapAllowed(dumpedRdbStore), UPDATE_RDB_RESULT.OK);
		ReplStage replStage = store.getMetaStore().getCurrentReplStage();
		Assert.assertEquals(replStage.getBeginGtidset(), new GtidSet(masterUuidA + ":1-100,"+ masterUuidB + ":1-100"));
		Assert.assertEquals(replStage.getGtidLost(), new GtidSet(masterUuidC + ":1-100"));
		ReplicationStoreMeta metaDup = store.getMetaStore().dupReplicationStoreMeta();
		Assert.assertEquals(metaDup.getRdbGtidSet(), masterUuidA + ":1-150," + masterUuidB + ":1-100");
		Assert.assertEquals((long)metaDup.getRdbBacklogOffset(), store.backlogEndOffset() - 1000);
		dumpedRdbStore.close();

	}

}
