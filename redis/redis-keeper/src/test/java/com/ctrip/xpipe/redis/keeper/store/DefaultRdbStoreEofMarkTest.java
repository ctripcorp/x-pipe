package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofMarkType;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * @author wenchao.meng
 *
 *         Jan 4, 2017
 */
public class DefaultRdbStoreEofMarkTest extends AbstractRedisKeeperTest {
	
	private File rdbFile;
	
	private DefaultRdbStore rdbStore;
	
	private String eofMark = randomKeeperRunid();
	
	@Before
	public void beforeDefaultRdbStoreEofMarkTest() throws IOException{
		
		rdbFile = new File(String.format("%s/%s.rdb", getTestFileDir(), getTestName()));
		rdbStore = new DefaultRdbStore(rdbFile, "replid", 0, new EofMarkType(eofMark));
	}

	
	@Test
	public void testRdbStoreEofMarkSend() throws IOException, InterruptedException{
	
		String data = randomString(1024);

		executors.execute(new AbstractExceptionLogTask() {
			
			@Override
			protected void doRun() throws Exception {
				
				rdbStore.writeRdb(Unpooled.wrappedBuffer(data.getBytes()));
				rdbStore.writeRdb(Unpooled.wrappedBuffer(eofMark.getBytes()));
				sleep(10);
				rdbStore.truncateEndRdb(eofMark.length());
				sleep(10);
			}
		});
		
		String rdbFileData = new String(readRdbFileTilEnd(rdbStore));
		Assert.assertEquals(data, rdbFileData);
		
	}
}
