package com.ctrip.xpipe.redis.keeper.store;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @author wenchao.meng
 *
 * Sep 12, 2016
 */
public class DefaultCommandStoreTest extends AbstractRedisKeeperTest{
	
	private DefaultCommandStore commandStore;
	
	private int maxFileSize = 1 << 10;
	
	private int minWritten = (1 << 20);

	@Before
	public void beforeDefaultCommandStoreTest() throws IOException{
		
		String testDir = getTestFileDir();
		File commandTemplate = new File(testDir, getTestName());
		commandStore = new DefaultCommandStore(commandTemplate, maxFileSize);
	}
	
	@Test
	public void testReadWrite() throws IOException{
		
		StringBuilder sb = new StringBuilder();
		final StringBuilder result = new StringBuilder();
		AtomicInteger totalWritten = new AtomicInteger();
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					commandStore.addCommandsListener(0, new CommandsListener() {
						
						@Override
						public void onCommand(ByteBuf byteBuf) {
							result.append(ByteBufUtils.readToString(byteBuf));
						}
						
						@Override
						public boolean isOpen() {
							return true;
						}
						
						@Override
						public void beforeCommand() {
							
						}
					});
				} catch (IOException e) {
					logger.error("[run]", e);
				}
			}
		}).start();
		
		while(true){
			int length = randomInt(100, 500);
			String random = randomString(length);
			sb.append(random);
			totalWritten.addAndGet(length);
			commandStore.appendCommands(Unpooled.wrappedBuffer(random.getBytes()));
			if(totalWritten.get() >= minWritten){
				logger.info("[testReadWrite][totalWritten]{}, {}", totalWritten, minWritten);
				break;
			}
		}
		
		sleep(500);
		logger.info("[testReadWrite]{}, {}",sb.length(), result.length());
		Assert.assertTrue(sb.toString().equals(result.toString()));
	}
	
}
