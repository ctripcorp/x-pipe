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
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultPsync;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午3:11:30
 */
public class PsyncTest extends AbstractRedisKeeperTest{
	
	private DefaultPsync psync;
	
	private ReplicationStoreManager replicationStoreManager;
	private DefaultReplicationStore replicationStore;

	private String masterId = randomString(40);
	private Long masterOffset;
	// MAGIC + AUX + SELECT DB
	private byte[] rdbHeader = new byte[] {0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x39, (byte) 0xfa, 0x09, 0x72,
			0x65, 0x64, 0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x36, 0x2e, 0x32, 0x2e, 0x36, (byte) 0xfe, 0x00};
	private String rdbContent = randomString();
	private String commandContent = randomString();
	
	private boolean isPartial = false;

	@Before
	public void beforePsyncTest() throws Exception{
		
		masterOffset = (long) randomInt(0, Integer.MAX_VALUE - 1);
		replicationStoreManager = createReplicationStoreManager();
		LifecycleHelper.initializeIfPossible(replicationStoreManager);
		replicationStore = (DefaultReplicationStore) replicationStoreManager.create();
		
		SimpleObjectPool<NettyClient> clientPool = NettyPoolUtil.createNettyPool(new DefaultEndPoint("127.0.0.1", 1234));
		psync = new DefaultPsync(clientPool, new DefaultEndPoint("127.0.0.1", 1234), replicationStoreManager, scheduled);
		psync.future().addListener(new CommandFutureListener<Object>() {
			
			@Override
			public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
				if(!commandFuture.isSuccess()){
					logger.error("[operationComplete]", commandFuture.cause());
				}
			}
		});
		psync.addPsyncObserver(new PsyncObserver() {
			@Override
			public void onFullSync(long masterRdbOffset) {
			}

			@Override
			public void reFullSync() {
			}

			@Override
			public void beginWriteRdb(EofType eofType, String replId, long masterRdbOffset) throws IOException {
			}

			@Override
			public void readAuxEnd(RdbStore rdbStore, Map<String, String> auxMap) {
				try {
					rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
					rdbStore.updateRdbType(RdbStore.Type.NORMAL);
					replicationStore.confirmRdb(rdbStore);
				} catch (Throwable th) {
					logger.info("[readAuxEnd][confirmRdb] fail", th);
				}
			}

			@Override
			public void endWriteRdb() {
			}

			@Override
			public void onContinue(String requestReplId, String responseReplId) {
			}

			@Override
			public void onKeeperContinue(String replId, long beginOffset) {
			}
		});
	}

	private void initRdbStore() throws IOException {
		RdbStore rdbStore = replicationStore.prepareRdb(masterId, masterOffset, new LenEofType(0));
		rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
		rdbStore.updateRdbType(RdbStore.Type.NORMAL);
		replicationStore.confirmRdb(rdbStore);
		replicationStore.getRdbStore().endRdb();
	}

	@Test
	public void testPsyncPartialeRight() throws XpipeException, IOException, InterruptedException{

		isPartial = true;
		byte [][]data = new byte[][]{
				("+" + DefaultPsync.PARTIAL_SYNC + "\r\n").getBytes(),
				commandContent.getBytes()
		};
		initRdbStore();
		
		runData(data);
	}

	@Test
	public void testPsync2() throws XpipeException, IOException, InterruptedException{

		isPartial = true;
		
		String newReplId = RunidGenerator.DEFAULT.generateRunid();
		byte [][]data = new byte[][]{
				("+" + DefaultPsync.PARTIAL_SYNC + " " + newReplId + "\r\n").getBytes(),
				commandContent.getBytes()
		};
		initRdbStore();
		
		Long secondReplIdOffset = replicationStore.getEndOffset() + 1;
		
		runData(data);
		
		Assert.assertEquals(newReplId, replicationStore.getMetaStore().getReplId());
		Assert.assertEquals(masterId, replicationStore.getMetaStore().getReplId2());
		Assert.assertEquals(secondReplIdOffset, replicationStore.getMetaStore().getSecondReplIdOffset());
	}

	@Test
	public void testPsync2Spaces() throws XpipeException, IOException, InterruptedException{

		isPartial = true;
		String newReplId = RunidGenerator.DEFAULT.generateRunid();

		byte [][]data = new byte[][]{
				("+" + DefaultPsync.PARTIAL_SYNC + "   " + newReplId + "  \r\n").getBytes(),
				commandContent.getBytes()
		};
		initRdbStore();
		
		Long secondReplIdOffset = replicationStore.getEndOffset() + 1;

		runData(data);
		
		Assert.assertEquals(newReplId, replicationStore.getMetaStore().getReplId());
		Assert.assertEquals(masterId, replicationStore.getMetaStore().getReplId2());
		Assert.assertEquals(secondReplIdOffset, replicationStore.getMetaStore().getSecondReplIdOffset());
	}
	
	@Test
	public void testPsync2SplitTcp() throws XpipeException, IOException, InterruptedException{

		isPartial = true;
		String newReplId = RunidGenerator.DEFAULT.generateRunid();

		byte [][]data = new byte[][]{
				("+" + DefaultPsync.PARTIAL_SYNC).getBytes(),
				("  " + newReplId + "  \r\n").getBytes(),
				commandContent.getBytes()
		};
		initRdbStore();
		
		Long secondReplIdOffset = replicationStore.getEndOffset() + 1;

		runData(data);
		
		Assert.assertEquals(newReplId, replicationStore.getMetaStore().getReplId());
		Assert.assertEquals(masterId, replicationStore.getMetaStore().getReplId2());
		Assert.assertEquals(secondReplIdOffset, replicationStore.getMetaStore().getSecondReplIdOffset());
	}

	
	@Test
	public void testPsyncEofMark() throws XpipeException, IOException, InterruptedException{
		
		String eof = RunidGenerator.DEFAULT.generateRunid();
		byte [][]data = new byte[][]{
				("+" + DefaultPsync.FULL_SYNC + " " + masterId + " " + masterOffset + "\r\n").getBytes(),
				("$EOF:" + eof + "\r\n").getBytes(),
				rdbHeader,
				rdbContent.getBytes(),
				eof.getBytes(),
				commandContent.getBytes()
		};
		
		runData(data);
	}

	
	@Test
	public void testPsyncFullRight() throws XpipeException, IOException, InterruptedException{

		int rdbLen = rdbContent.length() + rdbHeader.length;
		byte [][]data = new byte[][]{
				("+" + DefaultPsync.FULL_SYNC + " " + masterId + " " + masterOffset + "\r\n").getBytes(),
				("$" + rdbLen + "\r\n").getBytes(),
				rdbHeader,
				rdbContent.getBytes(),
				commandContent.getBytes()
		};
		
		runData(data);
	}

	@Test
	public void testPsyncFullWithRdbCrlf() throws XpipeException, IOException, InterruptedException{

		int rdbLen = rdbContent.length() + rdbHeader.length;
		byte [][]data = new byte[][]{
				("+" + DefaultPsync.FULL_SYNC + " " + masterId + " " + masterOffset + "\r\n").getBytes(),
				("$" + rdbLen + "\r\n").getBytes(),
				rdbHeader,
				(rdbContent + "\r\n").getBytes(),
				commandContent.getBytes()
		};
		
		runData(data);
	}

	@Test
	public void testPsyncFullWithSplit() throws XpipeException, IOException, InterruptedException{

		int rdbLen = rdbContent.length() + rdbHeader.length;
		byte [][]data = new byte[][]{
				("+" + DefaultPsync.FULL_SYNC + " " + masterId + " " + masterOffset + "\r\n").getBytes(),
				("$" + rdbLen).getBytes() ,
				"\r\n".getBytes(),
				rdbHeader,
				rdbContent.substring(0, rdbContent.length()/2).getBytes(),
				(rdbContent.substring(rdbContent.length()/2) + "\r\n").getBytes(),
				commandContent.substring(0, rdbContent.length()/2).getBytes(),
				commandContent.substring(rdbContent.length()/2).getBytes()
		};
		runData(data);
	}

	@Test
	public void testPsyncFailHalfRdb() throws Exception{
		
		psync.addFutureListener();

		int rdbLen = rdbContent.length() + rdbHeader.length;
		int midIndex = rdbContent.length()/2;
		byte [][]data = new byte[][]{
				("+" + DefaultPsync.FULL_SYNC + " " + masterId + " " + masterOffset + "\r\n").getBytes(),
				("$" + rdbLen + "\r\n").getBytes(),
				rdbHeader,
				(rdbContent.substring(0, midIndex) + "\r\n").getBytes(),
		};
		runData(data, false);
		
		psync.clientClosed(null);
		
		Assert.assertFalse(replicationStore.getRdbStore().checkOk());
	}

	private void runData(byte [][]data) throws XpipeException, IOException, InterruptedException {
		runData(data, true);
	}

	private void runData(byte [][]data, boolean assertResult) throws XpipeException, IOException, InterruptedException {
		
		ByteBuf []byteBufs = new ByteBuf[data.length];
		
		for(int i=0;i<data.length;i++){
			
			byte []bdata = data[i];
			
			byteBufs[i] = directByteBuf(bdata.length);
			byteBufs[i].writeBytes(bdata);
		}
		
		for(ByteBuf byteBuf : byteBufs){
			psync.receive(null, byteBuf);
		}

		if(assertResult){
			assertResult();
		}
		
	}

	private void assertResult() throws IOException, InterruptedException {
		replicationStore = (DefaultReplicationStore) replicationStoreManager.getCurrent();
		
		byte[] rdbResult = readRdbFileTilEnd(replicationStore);
		String commandResult = readCommandFileTilEnd(replicationStore, commandContent.length());
		
		if(!isPartial){
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			os.write(rdbHeader);
			os.write(rdbContent.getBytes());
			Assert.assertArrayEquals(os.toByteArray(), rdbResult);
		}else{
			Assert.assertTrue(rdbResult == null || rdbResult.length == 0);
		}
		Assert.assertEquals(commandContent, commandResult);
	}
}
