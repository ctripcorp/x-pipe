package com.ctrip.xpipe.redis.keeper.monitor;

import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.monitor.impl.DefaultCommandStoreDelay;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;

/**
 * @author wenchao.meng
 *
 *         Nov 24, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultCommandDelayTest extends AbstractRedisKeeperTest {

	private int delayLogLimitMicro = 5000;
	private int testCount = 100;
	private AtomicInteger logCount = new AtomicInteger();

	private DefaultCommandStoreDelay commandDelay;
	
	@Mock
	private CommandStore commandStore;

	@Before
	public void beforeCommandDelayTest() {
		commandDelay = new TestCommandDelay(commandStore, delayLogLimitMicro);
	}
	
	@Test
	public void testOffsetOverrideNoSend(){

		long offset = 0;
		for (int i = 0; i < DefaultCommandStoreDelay.SUPPORT_SLAVES -1; i++) {
			
			offset++;
			commandDelay.beginWrite();
			commandDelay.endWrite(offset);
		}
		
		commandDelay.beginWrite();
		commandDelay.endWrite(offset);

	}

	@Test
	public void testOffsetOverrideSendNoFlush(){

		long offset = 0;
		for (int i = 0; i < DefaultCommandStoreDelay.SUPPORT_SLAVES -1; i++) {
			
			offset++;
			commandDelay.beginWrite();
			commandDelay.endWrite(offset);
			commandDelay.beginSend(mock(CommandsListener.class), offset);
		}
		
		commandDelay.beginWrite();
		commandDelay.endWrite(offset);

	}

	@Test
	public void testEndWrite() {

		long offset = 0;

		commandDelay.beginWrite();
		commandDelay.endWrite(offset);

		Assert.assertEquals(0, logCount.get());

		for (int i = 0; i < testCount; i++) {
			offset++;
			commandDelay.beginWrite();
			sleep((delayLogLimitMicro / 1000) + 2);
			commandDelay.endWrite(offset);
			Assert.assertEquals(i + 1, logCount.get());
		}
	}

	@Test
	public void testBeginSend() {

		long offset = 0;
		long expectedLogCount = 0;

		for (int i = 0; i < testCount; i++) {

			offset++;
			commandDelay.beginWrite();
			commandDelay.endWrite(offset);
			sleep((delayLogLimitMicro / 1000) + 2);

			for (int j = 0; j < DefaultCommandStoreDelay.SUPPORT_SLAVES * 2; j++) {

				commandDelay.beginSend(mock(CommandsListener.class), offset);
				if (j < DefaultCommandStoreDelay.SUPPORT_SLAVES) {
					expectedLogCount++;
				}
				Assert.assertEquals(expectedLogCount, logCount.get());
			}
		}
	}

	@Test
	public void testFlush() {

		long offset = 0;

		for (int i = 0; i < testCount; i++) {

			offset++;
			CommandsListener commandsListener = mock(CommandsListener.class);
			
			commandDelay.beginWrite();
			commandDelay.endWrite(offset);

			commandDelay.beginSend(commandsListener, offset);
			sleep((delayLogLimitMicro / 1000) + 2);

			commandDelay.flushSucceed(commandsListener, offset);
			Assert.assertEquals(i+1, logCount.get());

			commandDelay.flushSucceed(mock(CommandsListener.class), offset);
			Assert.assertEquals(i+1, logCount.get());

		}
	}

	public class TestCommandDelay extends DefaultCommandStoreDelay {

		public TestCommandDelay(CommandStore commandStore, int delayLogLimitMicro) {
			super(commandStore, () -> delayLogLimitMicro);
		}

		@Override
		protected boolean logIfShould(long begin, long end, String message) {

			if (super.logIfShould(begin, end, message)) {
				logCount.incrementAndGet();
				return true;
			}
			return false;
		}
	}
}
