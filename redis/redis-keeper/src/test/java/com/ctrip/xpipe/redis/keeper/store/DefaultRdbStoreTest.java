package com.ctrip.xpipe.redis.keeper.store;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.store.RdbFileListener;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;

import io.netty.buffer.Unpooled;

/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public class DefaultRdbStoreTest extends AbstractRedisKeeperTest{
	
	private long rdbFileSize = 1024L;
	private AtomicLong readLen = new AtomicLong();

	@Test
	public void testNoDataBeginRead() throws IOException{
		
		String fileName = String.format("%s/%s.rdb", getTestFileDir(), getTestName());
		
		File file = new File(fileName);
		
		DefaultRdbStore rdbStore = new DefaultRdbStore(file, 1L, new LenEofType(rdbFileSize));

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
						public void setRdbFileInfo(EofType eofType, long rdbFileKeeperOffset) {
							logger.info("[setRdbFileInfo]{}, {}", eofType, rdbFileKeeperOffset);
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
						}
						
						@Override
						public void beforeFileData() {
							logger.info("[beforeFileData]");
						}
					});
				} catch (IOException e) {
					logger.error("[run][read rdb error]" + rdbStore, e);
				}
			}
		}).start();
	}

}
