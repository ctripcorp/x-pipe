package com.ctrip.xpipe.redis.console.notifier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.web.client.ResourceAccessException;

import static org.mockito.Mockito.*;



/**
 * @author shyin
 *
 *         Oct 28, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class MetaNotifyTaskTest {
	private MetaNotifyTask<Void> task;
	@Mock
	private Foo foo;
	
	private int retryTimes = 10;

	@Before
	public void setUp() {
		task = new MetaNotifyTask<Void>("TestMetaNotifyTask", retryTimes, new MetaNotifyRetryPolicy(100)) {

			@Override
			public Void doNotify() {
				foo.foo();
				return null;
			}
		};
	}

	@Test
	public void testMetaNotifyTaskSuccess() {
		task.run();
		verify(foo, times(1)).foo();
	}
	
	@Test
	public void testMetaNotifyTaskFail() {
		doThrow(new ResourceAccessException("test")).when(foo).foo();
		task.run();
		verify(foo,times(retryTimes + 1)).foo();
	}
	
	@Test
	public void testMetaNotifyTaskRetryFail() {
		doThrow(new RuntimeException("test")).when(foo).foo();
		task.run();
		verify(foo,times(1)).foo();
	}

	private class Foo {
		public void foo() {
		}
	}
}
