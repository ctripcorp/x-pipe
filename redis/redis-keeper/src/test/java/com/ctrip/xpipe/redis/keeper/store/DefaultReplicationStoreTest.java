package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.store.FullSyncListener;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class DefaultReplicationStoreTest extends AbstractRedisKeeperTest{

	private File baseDir;
	
	private DefaultReplicationStore store; 

	@Before
	public void beforeDefaultReplicationStoreTest() throws IOException{
		baseDir = new File(getTestFileDir());
	}

	@Test
	public void testInterruptedException() throws IOException {

		String keeperRunid = randomKeeperRunid();
		int dataLen = 100;
		store = new DefaultReplicationStore(baseDir, new DefaultKeeperConfig(), keeperRunid, createkeeperMonitor());
		RdbStore rdbStore = store.beginRdb(randomKeeperRunid(), -1, new LenEofType(dataLen));

		rdbStore.writeRdb(Unpooled.wrappedBuffer(randomString(dataLen).getBytes()));
		rdbStore.endRdb();

		Thread.currentThread().interrupt();
		store = new DefaultReplicationStore(baseDir, new DefaultKeeperConfig(), keeperRunid, createkeeperMonitor());


		//clear interrupt
		Thread.interrupted();

		store.appendCommands(Unpooled.wrappedBuffer(randomString(dataLen).getBytes()));
		store = new DefaultReplicationStore(baseDir, new DefaultKeeperConfig(), keeperRunid, createkeeperMonitor());

	}
	
	@Test
	public void testReadWhileDestroy() throws Exception{

		store = new DefaultReplicationStore(baseDir, new DefaultKeeperConfig(), randomKeeperRunid(), createkeeperMonitor());
		store.getMetaStore().becomeActive();

		int dataLen = 1000;
		RdbStore rdbStore = store.beginRdb(randomKeeperRunid(), -1, new LenEofType(dataLen));
		
		rdbStore.writeRdb(Unpooled.wrappedBuffer(randomString(dataLen).getBytes()));
		rdbStore.endRdb();
		
		CountDownLatch latch  = new CountDownLatch(2);
		AtomicBoolean result = new AtomicBoolean(true);
		
		executors.execute(new AbstractExceptionLogTask() {
			
			@Override
			protected void doRun() throws Exception {
				
				try{
					sleep(2);
					store.close();
					store.destroy();
				}finally{
					latch.countDown();
				}
			}
		});
		
	
		executors.execute(new AbstractExceptionLogTask() {
			
			@Override
			protected void doRun() throws Exception {
				
				try{
					store.fullSyncIfPossible(new FullSyncListener() {
						
						@Override
						public ChannelFuture onCommand(ReferenceFileRegion referenceFileRegion) {
							
							return null;
						}
						
						@Override
						public void beforeCommand() {
							
						}
						
						@Override
						public void setRdbFileInfo(EofType eofType, long rdbFileKeeperOffset) {
							
						}
						
						@Override
						public void onFileData(ReferenceFileRegion referenceFileRegion) throws IOException {
							sleep(10);
						}
						
						@Override
						public boolean isOpen() {
							return true;
						}
						
						@Override
						public void exception(Exception e) {
							logger.info("[exception][fail]" + e.getMessage());
							result.set(false);
						}
						
						@Override
						public void beforeFileData() {
							
						}

						@Override
						public Long processedOffset() {
							return null;
						}
					});
				}catch(Exception e){
					logger.info("[exception][fail]" + e.getMessage());
					result.set(false);
				}finally{
					latch.countDown();
				}
			}
		});
		
		
		latch.await(100, TimeUnit.MILLISECONDS);
		Assert.assertFalse(result.get());
	}

	
	@Test
	public void testReadWrite() throws Exception {

		store = new DefaultReplicationStore(baseDir, new DefaultKeeperConfig(), randomKeeperRunid(), createkeeperMonitor());
		store.getMetaStore().becomeActive();


		StringBuffer exp = new StringBuffer();

		int cmdCount = 4;
		int cmdLen = 10;

		store.beginRdb("master", -1, new LenEofType(-1));

		for (int j = 0; j < cmdCount; j++) {
			ByteBuf buf = Unpooled.buffer();
			String cmd = UUID.randomUUID().toString().substring(0, cmdLen);
			exp.append(cmd);
			buf.writeBytes(cmd.getBytes());
			store.getCommandStore().appendCommands(buf);
		}
		String result = readCommandFileTilEnd(store, exp.length());
		assertEquals(exp.toString(), result);
		store.close();
	}

	@Test
	public void testGcNotContinueRdb() throws Exception {
		TestKeeperConfig config = new TestKeeperConfig(100, 1, 1024, 0);
		store = new DefaultReplicationStore(baseDir, config, randomKeeperRunid(), createkeeperMonitor());
		store.getMetaStore().becomeActive();

		int dataLen = 100;
		RdbStore rdbStore = store.beginRdb(randomKeeperRunid(), -1, new LenEofType(dataLen));

		rdbStore.writeRdb(Unpooled.wrappedBuffer(randomString(dataLen).getBytes()));
		rdbStore.endRdb();

		IntStream.range(0,5).forEach(i -> {
			try {
				store.getCommandStore().appendCommands(Unpooled.wrappedBuffer(randomString(100).getBytes()));
			} catch (Exception e) {
				logger.info("[testGcNotContinueRdb][append cmd fail]", e);
			}
		});

		store.gc(); // just release cmd files
		Assert.assertNotNull(store.getRdbStore());

		store.gc();
		Assert.assertNull(store.getRdbStore());
		Assert.assertNull(store.getMetaStore().dupReplicationStoreMeta().getRdbFile());
	}
	
}
