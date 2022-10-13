package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.store.RdbFileListener;
import com.ctrip.xpipe.redis.core.store.ReplicationProgress;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public class DefaultRdbStoreTest extends AbstractRedisKeeperTest{
	
	private long rdbFileSize = 1024L;
	
	private AtomicLong readLen = new AtomicLong();
	
	private File rdbFile;
	private DefaultRdbStore rdbStore;
	private AtomicReference<Exception> exception = new AtomicReference<Exception>(null);
	
	@Before
	public void beforeDefaultRdbStoreTest() throws IOException{
		
		String fileName = String.format("%s/%s.rdb", getTestFileDir(), getTestName());
		rdbFile = new File(fileName);
		rdbStore = new DefaultRdbStore(rdbFile, 1L, new LenEofType(rdbFileSize));
	}
	
	@Test
	public void testFail() throws IOException, TimeoutException {
		
		readRdbInNewThread(rdbStore);
		byte[] message = randomString().getBytes();  
		rdbStore.writeRdb(Unpooled.wrappedBuffer(message));
		rdbStore.failRdb(new Exception("just fail it"));
		
		waitConditionUntilTimeOut(() -> exception.get() != null);
	}

	@Test
	public void testNoDataBeginRead() throws IOException{
		

		readRdbInNewThread(rdbStore);
		
		sleep(10);
		Assert.assertEquals(0, readLen.get());
		
		byte[] message = randomString().getBytes();  
		rdbStore.writeRdb(Unpooled.wrappedBuffer(message));
		rdbStore.endRdb();
		sleep(200);
		Assert.assertEquals(message.length, readLen.get());
	}

	private void readRdbInNewThread(final DefaultRdbStore rdbStore) {
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				
				try {
					rdbStore.readRdbFile(new RdbFileListener() {
						@Override
						public void setRdbFileInfo(EofType eofType, ReplicationProgress<?> rdbProgress) {
							logger.info("[setRdbFileInfo]{}, {}", eofType, rdbProgress);
						}

						@Override
						public boolean supportProgress(Class<? extends ReplicationProgress<?>> clazz) {
							return true;
						}

						@Override
						public void onFileData(ReferenceFileRegion referenceFileRegion) throws IOException {
							
							if(referenceFileRegion != null){
								logger.info("[onFileData]{}", referenceFileRegion);
								if(referenceFileRegion.count() > 0){
									readLen.addAndGet(referenceFileRegion.count());
									referenceFileRegion.release();
								}
							}
						}
						
						@Override
						public boolean isOpen() {
							return true;
						}
						
						@Override
						public void exception(Exception e) {
							logger.info("[exception]", e);
							exception.set(e);
						}
						
						@Override
						public void beforeFileData() {
							logger.info("[beforeFileData]");
						}
					});
				} catch (IOException e) {
					logger.error("[run][read rdb error]" + rdbStore, e);
					exception.set(e);
				}
			}
		}).start();
	}

}
