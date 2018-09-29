package com.ctrip.xpipe.command;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author wenchao.meng
 *
 *         Mar 6, 2017
 */
public class UnitlSuccessTest extends AbstractTest {

	private int totalCommand = 5;
	private int middle = 2;

	@Test
	public void testSuccess() throws InterruptedException, ExecutionException {

		@SuppressWarnings("rawtypes")
		List<Command> commands = new LinkedList<>();
		for (int i = 0; i < totalCommand; i++) {
			commands.add(new TestCommand(randomString()));
		}

		String result = (String) new UntilSuccess(commands).execute().get();
		Assert.assertEquals(((TestCommand) commands.get(0)).getSuccessMessage(), result);

		for (int i = 1; i < totalCommand; i++) {
			Assert.assertFalse(((TestCommand) commands.get(i)).isBeginExecute());
		}
	}

	@Test
	public void testFail() {

		@SuppressWarnings("rawtypes")
		List<Command> commands = new LinkedList<>();
		for (int i = 0; i < totalCommand; i++) {
			commands.add(new TestCommand(new Exception(randomString(10))));
		}

		try {
			new UntilSuccess(commands).execute().get();
			Assert.fail();
		} catch (ExecutionException e) {
		} catch (InterruptedException e) {
			Assert.fail();
		}
	}

	@Test
	public void testFailFistFinallySuccess() throws InterruptedException, ExecutionException {
		
		@SuppressWarnings("rawtypes")
		List<Command> commands = new LinkedList<>();
		for (int i = 0; i < totalCommand; i++) {
			if (i == middle) {
				commands.add(new TestCommand(randomString()));
			} else {
				commands.add(new TestCommand(new Exception(randomString(10))));
			}
		}

		String result = (String) new UntilSuccess(commands).execute().get();

		Assert.assertEquals(((TestCommand) commands.get(middle)).getSuccessMessage(), result);
		for (int i = middle + 1; i < totalCommand; i++) {
			Assert.assertFalse(((TestCommand) commands.get(i)).isBeginExecute());
		}

	}

	@Test
	public void testCancel() {
		
		@SuppressWarnings("rawtypes")
		List<Command> commands = new LinkedList<>();
		for (int i = 0; i < totalCommand; i++) {
			commands.add(new TestCommand(randomString(), 1000));
		}
		
		CommandFuture<Object> future = new UntilSuccess(commands).execute();
		
		future.cancel(true);
		
		Assert.assertTrue(commands.get(0).future().isCancelled());
		Assert.assertTrue(future.isCancelled());
		
		for (int i = 1; i < totalCommand; i++) {
			Assert.assertFalse(((TestCommand) commands.get(i)).isBeginExecute());
		}
	}

}
