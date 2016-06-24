package com.ctrip.xpipe.redis.keeper.protocal.cmd;



import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.core.protocal.cmd.Psync;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午3:11:30
 */
public class PsyncTest extends AbstractRedisKeeperTest{
	
	
	private ByteBufAllocator allocator  = ByteBufAllocator.DEFAULT;
	private Psync psync;
	
	private ReplicationStoreManager replicationStoreManager;
	private ReplicationStore replicationStore;

	private String masterId = randomString(40);
	private Long masterOffset = 1024L;
	private String rdbContent = randomString();
	private String commandContent = randomString();
	
	private boolean isPartial = false;

	@Before
	public void beforePsyncTest() throws IOException{
		
		replicationStoreManager = createReplicationStoreManager();
		replicationStore = replicationStoreManager.create();
		
		psync = new Psync(new DefaultEndPoint("127.0.0.1", 1234), createKeeperMeta(), replicationStoreManager);
	}

	@Test
	public void testPsyncPartialeRight() throws XpipeException, IOException, InterruptedException{

		isPartial = true;
		String []data = new String[]{
				"+" + Psync.PARTIAL_SYNC + "\r\n",
				commandContent
		};
		//create store
		replicationStore.beginRdb(masterId, masterOffset, 12345);
		replicationStore.endRdb();
		runData(data);
	}

	@Test
	public void testPsyncFullRight() throws XpipeException, IOException, InterruptedException{
		
		String []data = new String[]{
				"+" + Psync.FULL_SYNC + " " + masterId + " " + masterOffset + "\r\n",
				"$" + rdbContent.length() + "\r\n",
				rdbContent,
				commandContent
		};
		
		runData(data);
	}

	@Test
	public void testPsyncFullWithRdbCrlf() throws XpipeException, IOException, InterruptedException{
		
		String []data = new String[]{
				"+" + Psync.FULL_SYNC + " " + masterId + " " + masterOffset + "\r\n",
				"$" + rdbContent.length() + "\r\n",
				rdbContent + "\r\n",
				commandContent
		};
		
		runData(data);
	}

	@Test
	public void testPsyncFullWithSplit() throws XpipeException, IOException, InterruptedException{
		
		String []data = new String[]{
				"+" + Psync.FULL_SYNC + " " + masterId + " " + masterOffset + "\r\n",
				"$" + rdbContent.length() ,
				"\r\n",
				rdbContent.substring(0, rdbContent.length()/2),
				rdbContent.substring(rdbContent.length()/2) + "\r\n",
				commandContent.substring(0, rdbContent.length()/2),
				commandContent.substring(rdbContent.length()/2)
		};
		runData(data);
	}

	private void runData(String []data) throws XpipeException, IOException, InterruptedException {
		
		ByteBuf []byteBufs = new ByteBuf[data.length];
		
		for(int i=0;i<data.length;i++){
			
			byte []bdata = data[i].getBytes();
			
			byteBufs[i] = allocator.buffer(bdata.length); 
			byteBufs[i].writeBytes(bdata);
		}
		
		for(ByteBuf byteBuf : byteBufs){
			psync.handleResponse(null, byteBuf);
		}

		assertResult();
		
	}

	private void assertResult() throws IOException, InterruptedException {
		
		String rdbResult = readRdbFileTilEnd(replicationStore);
		String commandResult = readCommandFileTilEnd(replicationStore);
		
		if(!isPartial){
			Assert.assertEquals(rdbContent, rdbResult);
			System.out.println(commandContent);
		}else{
			Assert.assertTrue(rdbResult == null || rdbResult.length() == 0);
		}
		System.out.println(commandResult);
		Assert.assertEquals(commandContent, commandResult);
	}
}
