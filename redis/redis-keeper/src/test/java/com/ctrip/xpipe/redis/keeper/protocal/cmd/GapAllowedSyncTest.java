package com.ctrip.xpipe.redis.keeper.protocal.cmd;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.GapAllowedSync;
import com.ctrip.xpipe.redis.core.protocal.GapAllowedSyncObserver;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractGapAllowedSync;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultGapAllowedSync;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.Map;

import static com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant.REDIS_RDB_AUX_KEY_GTID_EXECUTED;
import static com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant.REDIS_RDB_AUX_KEY_REPL_MODE;

public class GapAllowedSyncTest extends AbstractRedisKeeperTest{
	private final String EMPTY_GTIDSET_REPR = "\"\"";
	private DefaultGapAllowedSync gasync;
	private ReplicationStoreManager replicationStoreManager;
	private ReplicationStore replicationStore;
	private String REPLID_EMPTY = "0000000000000000000000000000000000000000";
	private String replIdA = "000000000000000000000000000000000000000A";
	private String replIdB = "000000000000000000000000000000000000000B";
	private String replIdC = "000000000000000000000000000000000000000C";
	private String uuidA = "111111111111111111111111111111111111111A";
	private String uuidB = "111111111111111111111111111111111111111B";
	private String uuidC = "111111111111111111111111111111111111111C";
	private GapAllowedSyncStatObserver gasyncStatObserver;
	private String eof = RunidGenerator.DEFAULT.generateRunid();

	class GapAllowedSyncStatObserver implements GapAllowedSyncObserver {
		private int xFullsync = 0;
		private int xContinue = 0;
		private int switchToXsync = 0;
		private int switchToPsync = 0;
		private int updateXsync = 0;
		private int fullsync = 0;
		private int reFullsync = 0;
		private int pcontinue = 0;
		private int keeperContinue = 0;

		public int getXFullsync() {
			return xFullsync;
		}

		public int getXContinue() {
			return xContinue;
		}

		public int getSwitchToXsync() {
			return switchToXsync;
		}

		public int getSwitchToPsync() {
			return switchToPsync;
		}

		public int getUpdateXsync() {
			return updateXsync;
		}

		public int getFullsync() {
			return fullsync;
		}

		public int getReFullsync() {
			return reFullsync;
		}

		public int getContinue() {
			return pcontinue;
		}

		public int getKeeperContinue() {
			return keeperContinue;
		}

		@Override
		public void onXFullSync(String replId, long replOff, String masterUuid, GtidSet gtidLost) {
			xFullsync++;
		}

		@Override
		public void onXContinue(String replId, long replOff, String masterUuid, GtidSet gtidCont) {
			xContinue++;
		}

		@Override
		public void onSwitchToXsync(String replId, long replOff, String masterUuid, GtidSet gtidCont, GtidSet gtidLost) {
			switchToXsync++;
		}

		@Override
		public void onSwitchToPsync(String replId, long replOff) {
			switchToPsync++;
		}

		@Override
		public void onUpdateXsync() {
			updateXsync++;
		}

		@Override
		public void onFullSync(long masterRdbOffset) {
			fullsync++;
		}

		@Override
		public void reFullSync() {
			reFullsync++;
		}

		@Override
		public void onContinue(String requestReplId, String responseReplId) {
			pcontinue++;
		}

		@Override
		public void onKeeperContinue(String replId, long beginOffset) {
			keeperContinue++;
		}

		@Override
		public void beginWriteRdb(EofType eofType, String replId, long masterRdbOffset) throws IOException {

		}

		@Override
		public void readAuxEnd(RdbStore rdbStore, Map<String, String> auxMap) {
			String gtidExecuted = auxMap.get(RdbConstant.REDIS_RDB_AUX_KEY_GTID_EXECUTED);
			if (gtidExecuted != null) {
				logger.info("[readAuxEnd][gtid-executed] {}", gtidExecuted);
				rdbStore.updateRdbGtidSet(gtidExecuted);
			} else {
				String gtidSet = auxMap.getOrDefault(RdbConstant.REDIS_RDB_AUX_KEY_GTID, GtidSet.EMPTY_GTIDSET);
				logger.info("[readAuxEnd][gtid] {}", gtidSet);
				rdbStore.updateRdbGtidSet(gtidSet);
			}

			RdbStore.Type rdbType = auxMap.containsKey(RdbConstant.REDIS_RDB_AUX_KEY_RORDB) ? RdbStore.Type.RORDB : RdbStore.Type.NORMAL;
			logger.info("[readAuxEnd][rdb] {}", rdbType);
			rdbStore.updateRdbType(rdbType);

			try {
				if (rdbStore.isGapAllowed()) {
					replicationStoreManager.getCurrent().confirmRdbGapAllowed(rdbStore);
				} else {
					replicationStoreManager.getCurrent().confirmRdb(rdbStore);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void endWriteRdb() {

		}
	}

	@Before
	public void beforePsyncTest() throws Exception{
		replicationStoreManager = createReplicationStoreManager();
		LifecycleHelper.initializeIfPossible(replicationStoreManager);
		replicationStore = replicationStoreManager.create();

		SimpleObjectPool<NettyClient> clientPool = NettyPoolUtil.createNettyPool(new DefaultEndPoint("127.0.0.1", 1234));
		gasync = new DefaultGapAllowedSync(clientPool, new DefaultEndPoint("127.0.0.1", 1234), replicationStoreManager, scheduled);
		gasync.future().addListener(new CommandFutureListener<Object>() {
			@Override
			public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
				if(!commandFuture.isSuccess()){
					logger.error("[operationComplete]", commandFuture.cause());
				}
			}
		});
		gasyncStatObserver = new GapAllowedSyncStatObserver();
		gasync.addPsyncObserver(gasyncStatObserver);
	}

	private void writeRdbGenericString(ByteArrayOutputStream os, String str) throws IOException{
		int len = str.length();

		if (len < (1<<6)) {
			os.write((char)len);
		} else if (len < (1<<14)) {
			os.write(((len>>8)&0XFF)|(1<<6));
			os.write(len&0xFF);
		} else {
			throw new UnsupportedOperationException();
		}

		os.write(str.getBytes());
	}

	private byte[] generateAuxOnlyRdb(String replId, long replOff, ReplStage.ReplProto proto,
							   String gtidExecuted, int contentLen) throws IOException {
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		os.write("REDIS0009".getBytes()); //magic

		os.write((char)250); //RDB_OPCODE_AUX
		writeRdbGenericString(os, "replid");
		writeRdbGenericString(os, replId);

		os.write((char)0xfa); //RDB_OPCODE_AUX
		writeRdbGenericString(os, "reploff");
		writeRdbGenericString(os, String.valueOf(replOff));

		os.write((char)0xfa); //RDB_OPCODE_AUX
		writeRdbGenericString(os, REDIS_RDB_AUX_KEY_REPL_MODE);
		writeRdbGenericString(os, String.valueOf(proto));

		os.write((char)0xfa); //RDB_OPCODE_AUX
		writeRdbGenericString(os, REDIS_RDB_AUX_KEY_GTID_EXECUTED);
		writeRdbGenericString(os, gtidExecuted);

		os.write((char)0xfe); //RDB_OPCODE_SELECTDB
		os.write((char)0x00); //0

		os.write(randomString(contentLen).getBytes());

		os.write((char)0xff); // RDB_OPCODE_EOF
		for (int i = 0; i < 8; i++) {
			os.write((char)0x00);
		}

		return os.toByteArray();
	}

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

	private void runData(byte [][]data) throws XpipeException, IOException, InterruptedException {

		ByteBuf []byteBufs = new ByteBuf[data.length];

		for(int i=0;i<data.length;i++){

			byte []bdata = data[i];

			byteBufs[i] = directByteBuf(bdata.length);
			byteBufs[i].writeBytes(bdata);
		}

		for(ByteBuf byteBuf : byteBufs){
			gasync.receive(null, byteBuf);
		}
	}

	private void metaStoreAssertGapAllowed(MetaStore metaStore) {
		Assert.assertNull(metaStore.beginOffset());
		Assert.assertNull(metaStore.getReplId());
		Assert.assertNull(metaStore.getReplId2());
		Assert.assertNull(metaStore.getSecondReplIdOffset());
	}

	private void replStageAssertPsync(ReplStage replStage) {
		Assert.assertEquals(replStage.getProto(), ReplStage.ReplProto.PSYNC);
		Assert.assertEquals(replStage.getReplId().length(), 40);
		Assert.assertTrue(replStage.getBegOffsetRepl() >= 1);
		Assert.assertTrue(replStage.getBegOffsetBacklog() >= 0);
		Assert.assertEquals(replStage.getReplId2().length(), 40);
		Assert.assertTrue(replStage.getSecondReplIdOffset() >= -1);
		Assert.assertNull(replStage.getMasterUuid());
		assertNull(replStage.getBeginGtidset());
		assertNull(replStage.getGtidLost());
	}

	private void assertNull(GtidSet gtidSet) {
		Assert.assertTrue(gtidSet == null || StringUtil.trimEquals(gtidSet.toString(), "\"\""));
	}

	private void assertNotNull(GtidSet gtidSet) {
		Assert.assertTrue(gtidSet != null && !StringUtil.trimEquals(gtidSet.toString(), "\"\""));
	}

	private void replStageAssertXsync(ReplStage replStage) {
		Assert.assertEquals(replStage.getProto(), ReplStage.ReplProto.XSYNC);
		Assert.assertNotNull(replStage.getReplId());
		Assert.assertTrue(replStage.getBegOffsetRepl() > 0);
		Assert.assertTrue(replStage.getBegOffsetBacklog() >= 0);
		Assert.assertNull(replStage.getReplId2());
        Assert.assertEquals(replStage.getSecondReplIdOffset(), -1);
		Assert.assertNotNull(replStage.getMasterUuid());
		assertNotNull(replStage.getBeginGtidset());
	}

	// XFULLRESYNC REPLID <replidX> REPLOFF <reploffX> MASTER.UUID <uuidX> GTID.LOST <uuidX>:<gnoBaseX>-<gnoBaseX+gnoCountX-1>
	// RDB (gtid-set-executed: <uuidX>:<gnoBaseX+gnoCountX>-<gnoBaseX+2*gnoCountX-1>,<uuidP>:<gnoBaseP>-<gnoBaseP+gnoCountP-1>
	// GTID <uuidX>:gnoBaseX+2*gnoCountX SET FOO BAR ...(gnoCountX)...
	// CONTINUE <replidP> <replOffP>
	// ... (cmdLenP)...
	private	void setupReplicationStoreXP(String uuidX, String replidX, long replOffX, int gnoBaseX, int gnoCountX,
											String uuidP, String replidP, long replOffP, int gnoBaseP,
											int gnoCountP, int cmdLenP) throws IOException {
		int gnoLostX = gnoBaseX, gnoExecutedX = gnoBaseX + gnoCountX, gnoCmdX = gnoBaseX + 2*gnoCountX;
		String gtidLostRepr =  uuidX + ":" + gnoLostX + "-" + (gnoLostX + gnoCountX - 1);
		String gtidExecutedRepr = uuidX + ":" + gnoExecutedX + "-" + (gnoExecutedX+gnoCountX-1) + "," +
				uuidP + ":" + gnoBaseP + "-" + (gnoBaseP+ gnoCountP-1);

		Assert.assertTrue(replicationStore.isFresh());
		Assert.assertNull(replicationStore.getMetaStore().getPreReplStage());
		Assert.assertNull(replicationStore.getMetaStore().getCurrentReplStage());

		RdbStore rdbStore = replicationStore.prepareRdb(replidX, replOffX, new LenEofType(0),
				ReplStage.ReplProto.XSYNC, new GtidSet(gtidLostRepr), uuidX);
		rdbStore.updateRdbType(RdbStore.Type.NORMAL);
		rdbStore.updateRdbGtidSet(gtidExecutedRepr);
		replicationStore.confirmRdbGapAllowed(rdbStore);

		replicationStore.appendCommands(Unpooled.wrappedBuffer(generateGtidCommands(uuidX, gnoCmdX,gnoCountX)));

		long bakBacklogEndOff = replicationStore.backlogEndOffset();

		replicationStore.switchToPSync(replidP,replOffP);
		replicationStore.appendCommands(Unpooled.wrappedBuffer(generateVanillaCommands(cmdLenP)));

		Assert.assertFalse(replicationStore.isFresh());

		ReplStage curReplStage = replicationStore.getMetaStore().getCurrentReplStage();
		ReplStage prevReplStage = replicationStore.getMetaStore().getPreReplStage();

		replStageAssertXsync(prevReplStage);
		Assert.assertEquals(prevReplStage.getBeginGtidset().toString(), gtidExecutedRepr);
		Assert.assertEquals(prevReplStage.getGtidLost().toString(), gtidLostRepr);
		Assert.assertEquals(prevReplStage.getReplId(), replidX);
		Assert.assertEquals(prevReplStage.getBegOffsetBacklog(), 0);
		Assert.assertEquals(prevReplStage.getBegOffsetRepl(), replOffX+1);

		replStageAssertPsync(curReplStage);
		Assert.assertEquals(curReplStage.getReplId(), replidP);
		Assert.assertEquals(curReplStage.getReplId2(), ReplicationStoreMeta.EMPTY_REPL_ID);
		Assert.assertEquals(curReplStage.getSecondReplIdOffset(), ReplicationStoreMeta.DEFAULT_SECOND_REPLID_OFFSET);
		Assert.assertEquals(curReplStage.getBegOffsetBacklog(), bakBacklogEndOff);
		Assert.assertEquals(curReplStage.getBegOffsetRepl(), replOffP+1);
	}

	// FULLRESYNC <replidP> <replOffP>
	// ... (cmdLenP)...
	// XCONTINUE REPLID <replidX> REPLOFF <reploffX> MASTER.UUID <uuidX> GTID.SET <uuidX>:<gnoBaseX>-<gnoBaseX+gnoCountX-1>
	// GTID <uuidX>:gnoBaseX+gnoCountX SET FOO BAR ... (gnoCountX)
	private	void setupReplicationStorePX(String replidP, long replOffP, int cmdLenP,
											String uuidX, String replidX, long replOffX,
											int gnoBaseX, int gnoCountX) throws IOException {
		int gnoCmdX = gnoBaseX + gnoCountX;
		String gtidContRepr =  uuidX + ":" + gnoBaseX + "-" + (gnoBaseX + gnoCountX - 1);

		Assert.assertTrue(replicationStore.isFresh());
		Assert.assertNull(replicationStore.getMetaStore().getPreReplStage());
		Assert.assertNull(replicationStore.getMetaStore().getCurrentReplStage());

		RdbStore rdbStore = replicationStore.prepareRdb(replidP, replOffP, new LenEofType(0),
				ReplStage.ReplProto.PSYNC, null, null);
		rdbStore.updateRdbType(RdbStore.Type.NORMAL);
		rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
		replicationStore.confirmRdbGapAllowed(rdbStore);

		replicationStore.appendCommands(Unpooled.wrappedBuffer(generateVanillaCommands(cmdLenP)));

		long bakBacklogEndOff = replicationStore.backlogEndOffset();

		replicationStore.switchToXSync(replidX,replOffX,uuidX,new GtidSet(gtidContRepr), null);

		replicationStore.appendCommands(Unpooled.wrappedBuffer(generateGtidCommands(uuidX, gnoCmdX, gnoCountX)));

		Assert.assertFalse(replicationStore.isFresh());

		ReplStage curReplStage = replicationStore.getMetaStore().getCurrentReplStage();
		ReplStage prevReplStage = replicationStore.getMetaStore().getPreReplStage();

		replStageAssertPsync(prevReplStage);
		Assert.assertEquals(prevReplStage.getReplId(), replidP);
		Assert.assertEquals(prevReplStage.getReplId2(), ReplicationStoreMeta.EMPTY_REPL_ID);
		Assert.assertEquals(prevReplStage.getSecondReplIdOffset(), ReplicationStoreMeta.DEFAULT_SECOND_REPLID_OFFSET);
		Assert.assertEquals(prevReplStage.getBegOffsetBacklog(), 0);
		Assert.assertEquals(prevReplStage.getBegOffsetRepl(), replOffP+1);

		replStageAssertXsync(curReplStage);
		Assert.assertEquals(curReplStage.getBeginGtidset().toString(), gtidContRepr);
		Assert.assertTrue(curReplStage.getGtidLost().isEmpty());
		Assert.assertEquals(curReplStage.getReplId(), replidX);
		Assert.assertEquals(curReplStage.getBegOffsetBacklog(), bakBacklogEndOff);
		Assert.assertEquals(curReplStage.getBegOffsetRepl(), replOffX+1);
	}

	@Test
	public void testFullresync() throws XpipeException, IOException, InterruptedException{
		//	X(A), P(B) ==Fullresync(C)==> P(C)

		long beginReplOffP = 200000000;
		int cmdLenP = 1000, cmdLenC = 1000;

		setupReplicationStoreXP(uuidA, replIdA, 100000000, 1, 100,
				uuidB, replIdB, beginReplOffP, 1, 100, cmdLenP);

		long replOffC = 300000000;
		String reply = "+" + GapAllowedSync.FULL_SYNC + " " + replIdC + " " + replOffC + "\r\n";

		gasync.getRequest();

		runData(new byte[][]{
				reply.getBytes(),
				("$EOF:" + eof + "\r\n").getBytes(),
				generateAuxOnlyRdb(replIdC, replOffC, ReplStage.ReplProto.PSYNC, GtidSet.EMPTY_GTIDSET, 1000),
				eof.getBytes(),
				generateVanillaCommands(cmdLenC),
		});

		Assert.assertEquals(gasyncStatObserver.getFullsync(), 1);

		replicationStore = replicationStoreManager.getCurrent();

		Pair<GtidSet, GtidSet> gtidSet = replicationStore.getGtidSet();
		Assert.assertTrue(gtidSet.getKey().isEmpty());
		Assert.assertTrue(gtidSet.getValue().isEmpty());

		MetaStore metaStore = replicationStore.getMetaStore();
		metaStoreAssertGapAllowed(replicationStore.getMetaStore());
		Assert.assertEquals(metaStore.getCurReplStageReplId(), replIdC);

		ReplStage prevReplStage = replicationStore.getMetaStore().getPreReplStage();
		ReplStage curReplStage = replicationStore.getMetaStore().getCurrentReplStage();

		Assert.assertNull(prevReplStage);
		replStageAssertPsync(curReplStage);
		Assert.assertEquals(curReplStage.getReplId(), replIdC);
		Assert.assertEquals(curReplStage.getBegOffsetBacklog(), 0);
		Assert.assertEquals(curReplStage.getBegOffsetRepl(), replOffC+1);
		Assert.assertEquals(curReplStage.getReplId2(), REPLID_EMPTY);
		Assert.assertEquals(replicationStore.backlogEndOffset(), cmdLenC);
	}

	@Test
	public void testPsyncContinue() throws XpipeException, IOException, InterruptedException {
		//	X(A), P(B) ==psyncContinue(C)==> X(A), P(C)

		long replOffP = 200000000;
		int cmdLenP = 1000;

		setupReplicationStoreXP(uuidA, replIdA, 100000000, 1, 100,
				uuidB, replIdB, replOffP, 1, 100, cmdLenP);

		String reply = "+" + GapAllowedSync.PARTIAL_SYNC + " " + replIdC + "\r\n";

		gasync.getRequest();

		long originReplStageBegOffsetBacklog = replicationStore.getMetaStore().getCurrentReplStage().getBegOffsetBacklog();
		long originBacklogEndOffset = replicationStore.backlogEndOffset();

		runData(new byte[][]{
				reply.getBytes(),
				generateVanillaCommands(1000),
		});

		Assert.assertEquals(gasyncStatObserver.getContinue(), 1);

		replicationStore = replicationStoreManager.getCurrent();

		MetaStore metaStore = replicationStore.getMetaStore();
		metaStoreAssertGapAllowed(metaStore);

		ReplStage curReplStage = metaStore.getCurrentReplStage();
		replStageAssertPsync(curReplStage);
		Assert.assertEquals(curReplStage.getReplId(), replIdC);
		Assert.assertEquals(curReplStage.getBegOffsetRepl(), replOffP+1);
		Assert.assertEquals(curReplStage.getBegOffsetBacklog(), originReplStageBegOffsetBacklog);
		Assert.assertEquals(curReplStage.getReplId2(), replIdB);
		Assert.assertEquals(curReplStage.getSecondReplIdOffset(), replOffP+1+cmdLenP);

		ReplStage prevReplStage = metaStore.getPreReplStage();
		replStageAssertXsync(prevReplStage);

		Assert.assertEquals(replicationStore.backlogEndOffset(), originBacklogEndOffset+cmdLenP);
	}

	@Test
	public void testPsyncContinueFromOffsetSucc() throws XpipeException, IOException, InterruptedException {
		//	==psyncContinueFromOffset(C)==> fail

		long reploffC = 300000000;
		String reply = "+" + GapAllowedSync.PARTIAL_SYNC + " " + replIdC + " " + reploffC + "\r\n";

		gasync.getRequest();

		runData(new byte[][]{
				reply.getBytes(),
				generateVanillaCommands(1000),
		});

		Assert.assertFalse(gasync.future().isDone());

		Assert.assertEquals(gasyncStatObserver.getKeeperContinue(), 1);

		replicationStore = replicationStoreManager.getCurrent();

		MetaStore metaStore = replicationStore.getMetaStore();
		metaStoreAssertGapAllowed(metaStore);

		ReplStage curReplStage = metaStore.getCurrentReplStage();
		Assert.assertEquals(curReplStage.getProto(), ReplStage.ReplProto.PSYNC);
		Assert.assertEquals(curReplStage.getReplId(), replIdC);
		Assert.assertEquals(curReplStage.getBegOffsetRepl(), reploffC);
	}

	@Test
	public void testStoreNotFresh_psyncContinueFromOffsetFail() throws XpipeException, IOException, InterruptedException {
		//	X(A), P(B) ==psyncContinueFromOffset(C)==> fail

		long replOffP = 200000000;
		int cmdLenP = 1000;

		setupReplicationStoreXP(uuidA, replIdA, 100000000, 1, 100,
				uuidB, replIdB, replOffP, 1, 100, cmdLenP);

		long reploffC = 300000000;
		String reply = "+" + GapAllowedSync.PARTIAL_SYNC + " " + replIdC + " " + reploffC + "\r\n";

		gasync.getRequest();

		runData(new byte[][]{
				reply.getBytes(),
				generateVanillaCommands(1000),
		});

		Assert.assertTrue(gasync.future().isDone() && !gasync.future().isSuccess());
		Assert.assertTrue(gasync.future().cause() instanceof IllegalStateException);
	}

	@Test
	public void testSwitchToPsync() throws XpipeException, IOException, InterruptedException {
		//	P(A), X(B) ==switchToPsync(C)==> X(B), P(C)

		setupReplicationStorePX(replIdA, 100000000, 1000,
				uuidB, replIdB, 200000000, 1, 100);

		long replOffC = 300000000;
		int cmdLenC = 1000;
		String reply = "+" + GapAllowedSync.PARTIAL_SYNC + " " + replIdC + " " + replOffC + "\r\n";

		gasync.getRequest();

		long bakBacklogEndOffset = replicationStore.backlogEndOffset();

		runData(new byte[][]{
				reply.getBytes(),
				generateVanillaCommands(cmdLenC),
		});

		Assert.assertEquals(gasyncStatObserver.getSwitchToPsync(), 1);

		replicationStore = replicationStoreManager.getCurrent();

		MetaStore metaStore = replicationStore.getMetaStore();
		metaStoreAssertGapAllowed(metaStore);

		ReplStage curReplStage = metaStore.getCurrentReplStage();
		replStageAssertPsync(curReplStage);
		Assert.assertEquals(curReplStage.getReplId(), replIdC);
		Assert.assertEquals(curReplStage.getBegOffsetRepl(), replOffC+1);
		Assert.assertEquals(curReplStage.getBegOffsetBacklog(), bakBacklogEndOffset);
		Assert.assertEquals(curReplStage.getReplId2(), ReplicationStoreMeta.EMPTY_REPL_ID);
		Assert.assertEquals(curReplStage.getSecondReplIdOffset(), ReplicationStoreMeta.DEFAULT_SECOND_REPLID_OFFSET);

		ReplStage prevReplStage = metaStore.getPreReplStage();
		replStageAssertXsync(prevReplStage);
		Assert.assertEquals(prevReplStage.getReplId(), replIdB);

		Assert.assertEquals(replicationStore.backlogEndOffset(), bakBacklogEndOffset+cmdLenC);
	}

	@Test
	public void testXFullresync() throws XpipeException, IOException, InterruptedException{
		RdbStore rdbStore = replicationStore.prepareRdb(replIdA, 0, new LenEofType(0),
				ReplStage.ReplProto.PSYNC, null, null);
		rdbStore.updateRdbType(RdbStore.Type.NORMAL);
		replicationStore.confirmRdbGapAllowed(rdbStore);

		long replOff = 2000;
		String gtidExecutedRepr = uuidA + ":1-100" + "," + uuidB + ":1-100";
		String gtidLostRepr = uuidC + ":1-100";
		String reply = "+" + GapAllowedSync.XFULL_SYNC + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_REPLID + " " + replIdB + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_REPLOFF + " " + replOff + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_MASTER_UUID + " " + uuidB + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_GTID_LOST + " " + gtidLostRepr + " " +
				"\r\n";

		gasync.getRequest();

		runData(new byte[][]{
				reply.getBytes(),
				("$EOF:" + eof + "\r\n").getBytes(),
				generateAuxOnlyRdb(replIdB, replOff, ReplStage.ReplProto.XSYNC, gtidExecutedRepr, 1024),
				eof.getBytes(),
				generateGtidCommands(uuidB, 101, 100),
		});

		Assert.assertEquals(gasyncStatObserver.getXFullsync(), 1);

		replicationStore = replicationStoreManager.getCurrent();

		String gtidExecutedExpected = uuidA + ":1-100" + "," + uuidB + ":1-200";

		Pair<GtidSet, GtidSet> gtidSet = replicationStore.getGtidSet();
		Assert.assertEquals(gtidSet.getKey().toString(), gtidExecutedExpected);
		Assert.assertEquals(gtidSet.getValue().toString(), gtidLostRepr);

		MetaStore metaStore = replicationStore.getMetaStore();
		metaStoreAssertGapAllowed(metaStore);
		Assert.assertEquals(metaStore.getCurReplStageReplId(), replIdB);

		ReplStage prevReplStage = replicationStore.getMetaStore().getPreReplStage();
		ReplStage curReplStage = replicationStore.getMetaStore().getCurrentReplStage();
		Assert.assertNull(prevReplStage);
		replStageAssertXsync(curReplStage);
		Assert.assertEquals(curReplStage.getReplId(), replIdB);
		Assert.assertEquals(curReplStage.getBegOffsetBacklog(), 0);
		Assert.assertEquals(curReplStage.getBegOffsetRepl(), replOff+1);
		Assert.assertEquals(curReplStage.getMasterUuid(), uuidB);
		Assert.assertEquals(curReplStage.getBeginGtidset().toString(), gtidExecutedRepr);
		Assert.assertEquals(curReplStage.getGtidLost().toString(), gtidLostRepr);
	}

	@Test
	public void testXsyncContineFrom() throws XpipeException, IOException, InterruptedException {
		//	==XContinue(C)==> X(C)

		int gnoBaseX = 1, gnoCountX = 100;

		long replOffC = 300000000;
		String gtidBaseRepr = uuidB + ":" + gnoBaseX + "-" + (gnoBaseX+2*gnoCountX-1);
		String gtidLostRepr = uuidC + ":" + gnoBaseX + "-" + (gnoBaseX+gnoCountX-1);
		String gtidContRepr = gtidBaseRepr + "," + gtidLostRepr;

		String reply = "+" + GapAllowedSync.XPARTIAL_SYNC + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_REPLID + " " + replIdC + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_REPLOFF + " " + replOffC + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_MASTER_UUID + " " + uuidC + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_GTID_SET + " " + gtidContRepr + " " +
				"\r\n";

		gasync.getRequest();

		byte[] rawCmds = generateGtidCommands(uuidC, gnoBaseX+gnoCountX, gnoCountX);
		runData(new byte[][]{
				reply.getBytes(),
				rawCmds
		});

		Assert.assertEquals(gasyncStatObserver.getSwitchToXsync(), 1);

		replicationStore = replicationStoreManager.getCurrent();

		MetaStore metaStore = replicationStore.getMetaStore();
		metaStoreAssertGapAllowed(metaStore);

		ReplicationStoreMeta metaDup = metaStore.dupReplicationStoreMeta();
		Assert.assertNull(metaDup.getRdbFile());

		ReplStage curReplStage = metaStore.getCurrentReplStage();
		replStageAssertXsync(curReplStage);
		Assert.assertEquals(curReplStage.getReplId(), replIdC);
		Assert.assertEquals(curReplStage.getBegOffsetRepl(), replOffC+1);
		Assert.assertEquals(curReplStage.getBegOffsetBacklog(), 0);
		Assert.assertEquals(curReplStage.getBeginGtidset().toString(), gtidContRepr);
		Assert.assertTrue(curReplStage.getGtidLost().isEmpty());

		Assert.assertNull(metaStore.getPreReplStage());
	}

	@Test
	public void testXsyncContine() throws XpipeException, IOException, InterruptedException {
		//	P(A), X(B) ==XContinue(C)==> P(A), X(C)

		int gnoBaseX = 1, gnoCountX = 100;
		setupReplicationStorePX(replIdA, 100000000, 1000,
				uuidB, replIdB, 200000000, 1, 100);

		long replOffC = 300000000;
		String gtidBaseRepr = uuidB + ":" + gnoBaseX + "-" + (gnoBaseX+2*gnoCountX-1);
		String gtidLostRepr = uuidC + ":" + gnoBaseX + "-" + (gnoBaseX+gnoCountX-1);
		String gtidContRepr = gtidBaseRepr + "," + gtidLostRepr;
		String reply = "+" + GapAllowedSync.XPARTIAL_SYNC + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_REPLID + " " + replIdC + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_REPLOFF + " " + replOffC + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_MASTER_UUID + " " + uuidC + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_GTID_SET + " " + gtidContRepr + " " +
				"\r\n";

		gasync.getRequest();

		long bakBacklogEndOffset = replicationStore.backlogEndOffset();
		GtidSet bakBeginGtidSet = replicationStore.getMetaStore().getCurrentReplStage().getBeginGtidset();

		byte[] rawCmds = generateGtidCommands(uuidC, gnoBaseX+gnoCountX, gnoCountX);
		runData(new byte[][]{
				reply.getBytes(),
				rawCmds
		});

		Assert.assertEquals(gasyncStatObserver.getXContinue(), 1);
		Assert.assertEquals(gasyncStatObserver.getUpdateXsync(), 1);

		replicationStore = replicationStoreManager.getCurrent();

		MetaStore metaStore = replicationStore.getMetaStore();
		metaStoreAssertGapAllowed(metaStore);

		ReplStage curReplStage = metaStore.getCurrentReplStage();
		replStageAssertXsync(curReplStage);
		Assert.assertEquals(curReplStage.getReplId(), replIdC);
		long curBeginReplOff = curReplStage.getBegOffsetRepl() - curReplStage.getBegOffsetBacklog() + replicationStore.backlogEndOffset();
		Assert.assertEquals(curBeginReplOff, replOffC+1+rawCmds.length);
		Assert.assertEquals(curReplStage.getBegOffsetBacklog() + rawCmds.length, bakBacklogEndOffset);
		Assert.assertEquals(curReplStage.getBeginGtidset(), bakBeginGtidSet);
		Assert.assertEquals(curReplStage.getGtidLost().toString(), gtidLostRepr);

		ReplStage prevReplStage = metaStore.getPreReplStage();
		replStageAssertPsync(prevReplStage);
		Assert.assertEquals(prevReplStage.getReplId(), replIdA);

		Assert.assertTrue(replicationStore.backlogEndOffset() > bakBacklogEndOffset);
		Assert.assertEquals(replicationStore.getGtidSet().getValue().toString(), gtidLostRepr);

		String gtidNewExecutedRepr = uuidC + ":" + (gnoBaseX+gnoCountX) + "-" + (gnoBaseX+2*gnoCountX-1);
		String gtidExecutedRepr = gtidBaseRepr + "," + gtidNewExecutedRepr;
		Assert.assertEquals(replicationStore.getGtidSet().getKey().toString(), gtidExecutedRepr);
	}

	@Test
	public void testSwitchToXsync() throws XpipeException, IOException, InterruptedException {
		long replOffP = 200000000;
		int cmdLenP = 1000;
		int gnoBaseX = 1, gnoCountX = 100;

		setupReplicationStoreXP(uuidA, replIdA, 100000000, 1, 100,
				uuidB, replIdB, replOffP, 1, 100, cmdLenP);

		long replOffC = 300000000;
		String gtidContRepr = uuidC + ":" + gnoBaseX + "-" + (gnoBaseX+gnoCountX-1);
		String reply = "+" + GapAllowedSync.XPARTIAL_SYNC + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_REPLID + " " + replIdC + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_REPLOFF + " " + replOffC + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_MASTER_UUID + " " + uuidC + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_GTID_SET + " " + gtidContRepr + " " +
				"\r\n";

		long bakBacklogEndOffset = replicationStore.backlogEndOffset();

		gasync.getRequest();
		byte[] rawCmds = generateGtidCommands(uuidC, gnoBaseX+gnoCountX, gnoCountX);
		runData(new byte[][]{
				reply.getBytes(),
				rawCmds
		});

		Assert.assertEquals(gasyncStatObserver.getSwitchToXsync(), 1);

		replicationStore = replicationStoreManager.getCurrent();

		MetaStore metaStore = replicationStore.getMetaStore();
		metaStoreAssertGapAllowed(metaStore);

		ReplStage curReplStage = metaStore.getCurrentReplStage();
		ReplStage prevReplStage = metaStore.getPreReplStage();

		replStageAssertXsync(curReplStage);
		Assert.assertEquals(curReplStage.getBegOffsetRepl(), replOffC+1);
		Assert.assertEquals(curReplStage.getBegOffsetBacklog(), bakBacklogEndOffset);
		Assert.assertEquals(curReplStage.getBeginGtidset().toString(), gtidContRepr);
		Assert.assertTrue(curReplStage.getGtidLost().isEmpty());

		replStageAssertPsync(prevReplStage);
		Assert.assertEquals(prevReplStage.getReplId(), replIdB);
	}

	@Test
	public void testCheckReplIdNotThrowUnexpectedReplIdException_switchToXsync() throws Exception {
		int gnoBaseX = 1, gnoCountX = 100;
		setupReplicationStorePX(replIdA, 100000000, 1000,
				uuidB, replIdB, 200000000, 1, 100);

		long replOffC = 300000000;
		String gtidBaseRepr = uuidB + ":" + gnoBaseX + "-" + (gnoBaseX+2*gnoCountX-1);
		String gtidLostRepr = uuidC + ":" + gnoBaseX + "-" + (gnoBaseX+gnoCountX-1);
		String gtidContRepr = gtidBaseRepr + "," + gtidLostRepr;
		String reply = "+" + GapAllowedSync.XPARTIAL_SYNC + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_REPLID + " " + replIdC + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_REPLOFF + " " + replOffC + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_MASTER_UUID + " " + uuidC + " " +
				AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_GTID_SET + " " + gtidContRepr + " " +
				"\r\n";

		gasync.getRequest();

		byte[] rawCmds = generateGtidCommands(uuidC, gnoBaseX+gnoCountX, gnoCountX);
		runData(new byte[][]{
				reply.getBytes(),
				rawCmds
		});

		replicationStore = replicationStoreManager.getCurrent();

		replicationStore.checkReplId(replIdA);
	}
}
