package com.ctrip.xpipe.redis.protocal.cmd;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.AbstractRedisTest;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午3:11:30
 */
public class PsyncTest extends AbstractRedisTest{
	
	
	private ByteBufAllocator allocator  = ByteBufAllocator.DEFAULT;
	private Psync psync;
	private ReplicationStore replicationStore;

	private String masterId = randomString(40);
	private Long masterOffset = 1024L;
	private String rdbContent = randomString();
	private String commandContent = randomString();
	
	private boolean isPartial = false;

	@Before
	public void beforePsyncTest(){
		
		replicationStore = createReplicationStore();
		psync = new Psync(null, replicationStore);
	}

	@Test
	public void testPsyncPartialeRight() throws XpipeException{

		isPartial = true;
		String []data = new String[]{
				"+" + Psync.PARTIAL_SYNC + "\r\n",
				commandContent
		};
		
		runData(data);
	}

	@Test
	public void testPsyncFullRight() throws XpipeException{
		
		String []data = new String[]{
				"+" + Psync.FULL_SYNC + " " + masterId + " " + masterOffset + "\r\n",
				"$" + rdbContent.length() + "\r\n",
				rdbContent,
				commandContent
		};
		
		runData(data);
	}

	@Test
	public void testPsyncFullWithRdbCrlf() throws XpipeException{
		
		String []data = new String[]{
				"+" + Psync.FULL_SYNC + " " + masterId + " " + masterOffset + "\r\n",
				"$" + rdbContent.length() + "\r\n",
				rdbContent + "\r\n",
				commandContent
		};
		
		runData(data);
	}

	@Test
	public void testPsyncFullWithSplit() throws XpipeException{
		
		String []data = new String[]{
				"+" + Psync.FULL_SYNC + " " + masterId + " " + masterOffset + "\r\n",
				"$" + rdbContent.length() ,
				"\r\n",
				rdbContent.substring(0, rdbContent.length()/2),
				rdbContent.substring(rdbContent.length()/2),
				"\r\n",
				commandContent.substring(0, rdbContent.length()/2),
				commandContent.substring(rdbContent.length()/2)
		};
		runData(data);
	}

	private void runData(String []data) throws XpipeException {
		
		ByteBuf []byteBufs = new ByteBuf[data.length];
		
		for(int i=0;i<data.length;i++){
			
			byte []bdata = data[i].getBytes();
			
			byteBufs[i] = allocator.buffer(bdata.length); 
			byteBufs[i].writeBytes(bdata);
		}
		
		for(ByteBuf byteBuf : byteBufs){
			psync.handleResponse(byteBuf);
		}

		assertResult();
		
	}

	private void assertResult() {
		
		String rdbResult = readFileAsString(getRdbFile());
		String commandResult = readFileAsString(getCommandFile());
		
		if(!isPartial){
			Assert.assertEquals(rdbContent, rdbResult);
			System.out.println(commandContent);
		}else{
			Assert.assertEquals(null, rdbResult);
		}
		System.out.println(commandResult);
		Assert.assertEquals(commandContent, commandResult);
	}
	
	@Test
	public void testReadFile(){
		
		String content = readFileAsString("/data/xpipe/file");
		System.out.println(content);
		
	}
	
}
