package com.ctrip.xpipe.concurrent;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.verification.AtLeast;

import static org.mockito.Mockito.*;

import org.mockito.runners.MockitoJUnitRunner;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.retry.RetryTemplate;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.retry.RetryDelay;
import com.ctrip.xpipe.retry.RetryNTimes;

/**
 * @author wenchao.meng
 *
 * Sep 14, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class OneThreadTaskExecutorTest extends AbstractTest{
	
	@Mock
	private Command<Void> command;
	
	protected OneThreadTaskExecutor oneThreadTaskExecutor = new OneThreadTaskExecutor("unit test");
	
	@Test
	public void testStart(){
		oneThreadTaskExecutor.executeCommand(command);
		verify(command).execute();
		
	}

	@Test
	public void testStartMtimes(){
		
		int times = 100;
		for(int i=0;i<times;i++){
			oneThreadTaskExecutor.executeCommand(command);
			sleep(10);
			verify(command, times(i + 1)).execute();
		}
	}
	
	@Test
	public void testRetryTemplate(){
		
		int retryTimes = 10;
		CommandFuture<Void> future = new DefaultCommandFuture<>();
		when(command.execute()).thenReturn(future);
		future.setFailure(new Exception());
		
		RetryTemplate<Void> retryTemplate = new RetryNTimes<>(retryTimes);
		
		OneThreadTaskExecutor oneThreadTaskExecutor = new OneThreadTaskExecutor(retryTemplate, getTestName());
		
		oneThreadTaskExecutor.executeCommand(command);
		
		sleep(100);
		verify(command, times(retryTimes + 1)).execute();
	}
	
	@Test
	public void testClose() throws Exception{

		CommandFuture<Void> future = new DefaultCommandFuture<>();
		when(command.execute()).thenReturn(future);
		future.setFailure(new Exception());
		
		RetryTemplate<Void> retryTemplate = RetryNTimes.retryForEver(new RetryDelay(10));
		
		OneThreadTaskExecutor oneThreadTaskExecutor = new OneThreadTaskExecutor(retryTemplate, getTestName());
		
		oneThreadTaskExecutor.executeCommand(command);
		sleep(100);
		oneThreadTaskExecutor.destroy();
		verify(command, new AtLeast(1)).execute();
		verify(command, new AtLeast(1)).reset();
		
		sleep(100);
		verifyNoMoreInteractions(command);
	}
	
	
}
