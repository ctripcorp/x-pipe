package com.ctrip.xpipe.redis.keeper.protocal.cmd;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultPsync;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

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
	}

	@Test
	public void testPsyncPartialeRight() throws XpipeException, IOException, InterruptedException{

		isPartial = true;
		String []data = new String[]{
				"+" + DefaultPsync.PARTIAL_SYNC + "\r\n",
				commandContent
		};
		//create store
		replicationStore.beginRdb(masterId, masterOffset, new LenEofType(0));
		replicationStore.getRdbStore().endRdb();
		
		runData(data);
	}

	@Test
	public void testPsync2() throws XpipeException, IOException, InterruptedException{

		isPartial = true;
		
		String newReplId = RunidGenerator.DEFAULT.generateRunid();
		String []data = new String[]{
				"+" + DefaultPsync.PARTIAL_SYNC + " " + newReplId + "\r\n",
				commandContent
		};
		//create store
		replicationStore.beginRdb(masterId, masterOffset, new LenEofType(0));
		replicationStore.getRdbStore().endRdb();
		
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

		String []data = new String[]{
				"+" + DefaultPsync.PARTIAL_SYNC + "   " + newReplId + "  \r\n",
				commandContent
		};
		//create store
		replicationStore.beginRdb(masterId, masterOffset, new LenEofType(0));
		replicationStore.getRdbStore().endRdb();
		
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

		String []data = new String[]{
				"+" + DefaultPsync.PARTIAL_SYNC , 
				"  " + newReplId + "  \r\n",
				commandContent
		};
		//create store
		replicationStore.beginRdb(masterId, masterOffset, new LenEofType(0));
		replicationStore.getRdbStore().endRdb();
		
		Long secondReplIdOffset = replicationStore.getEndOffset() + 1;

		runData(data);
		
		Assert.assertEquals(newReplId, replicationStore.getMetaStore().getReplId());
		Assert.assertEquals(masterId, replicationStore.getMetaStore().getReplId2());
		Assert.assertEquals(secondReplIdOffset, replicationStore.getMetaStore().getSecondReplIdOffset());
	}

	
	@Test
	public void testPsyncEofMark() throws XpipeException, IOException, InterruptedException{
		
		String eof = RunidGenerator.DEFAULT.generateRunid();
		String []data = new String[]{
				"+" + DefaultPsync.FULL_SYNC + " " + masterId + " " + masterOffset + "\r\n",
				"$EOF:" + eof + "\r\n",
				rdbContent,
				eof,
				commandContent
		};
		
		runData(data);
	}

	@Test
	public void testPsyncFullRight() throws XpipeException, IOException, InterruptedException{
		
		String []data = new String[]{
				"+" + DefaultPsync.FULL_SYNC + " " + masterId + " " + masterOffset + "\r\n",
				"$" + rdbContent.length() + "\r\n",
				rdbContent,
				commandContent
		};
		
		runData(data);
	}

	@Test
	public void testPsyncFullWithRdbCrlf() throws XpipeException, IOException, InterruptedException{
		
		String []data = new String[]{
				"+" + DefaultPsync.FULL_SYNC + " " + masterId + " " + masterOffset + "\r\n",
				"$" + rdbContent.length() + "\r\n",
				rdbContent + "\r\n",
				commandContent
		};
		
		runData(data);
	}

	@Test
	public void testPsyncFullWithSplit() throws XpipeException, IOException, InterruptedException{
		
		String []data = new String[]{
				"+" + DefaultPsync.FULL_SYNC + " " + masterId + " " + masterOffset + "\r\n",
				"$" + rdbContent.length() ,
				"\r\n",
				rdbContent.substring(0, rdbContent.length()/2),
				rdbContent.substring(rdbContent.length()/2) + "\r\n",
				commandContent.substring(0, rdbContent.length()/2),
				commandContent.substring(rdbContent.length()/2)
		};
		runData(data);
	}

	@Test
	public void testPsyncFailHalfRdb() throws XpipeException, IOException, InterruptedException{
		
		psync.addFutureListener();
		
		int midIndex = rdbContent.length()/2;
		String []data = new String[]{
				"+" + DefaultPsync.FULL_SYNC + " " + masterId + " " + masterOffset + "\r\n",
				"$" + rdbContent.length() + "\r\n",
				rdbContent.substring(0, midIndex) + "\r\n",
		};
		runData(data, false);
		
		psync.clientClosed(null);
		
		Assert.assertFalse(replicationStore.getRdbStore().checkOk());
	}

	private void runData(String []data) throws XpipeException, IOException, InterruptedException {
		runData(data, true);
	}

	private void runData(String []data, boolean assertResult) throws XpipeException, IOException, InterruptedException {
		
		ByteBuf []byteBufs = new ByteBuf[data.length];
		
		for(int i=0;i<data.length;i++){
			
			byte []bdata = data[i].getBytes();
			
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
		
		String rdbResult = readRdbFileTilEnd(replicationStore);
		String commandResult = readCommandFileTilEnd(replicationStore, commandContent.length());
		
		if(!isPartial){
			Assert.assertEquals(rdbContent, rdbResult);
		}else{
			Assert.assertTrue(rdbResult == null || rdbResult.length() == 0);
		}
		Assert.assertEquals(commandContent, commandResult);
	}
}
