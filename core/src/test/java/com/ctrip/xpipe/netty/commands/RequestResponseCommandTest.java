package com.ctrip.xpipe.netty.commands;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.command.CommandTimeoutException;
import com.ctrip.xpipe.pool.BorrowObjectException;
import com.ctrip.xpipe.pool.XpipeObjectPool;
import com.ctrip.xpipe.simpleserver.Server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 *         Jul 1, 2016
 */
public class RequestResponseCommandTest extends AbstractTest {

	private Server server;
	private XpipeObjectPool<NettyClient> clientPool;

	@Before
	public void beforeRequestResponseCommandTest() throws Exception {

		server = startEchoServer();
		clientPool = new XpipeObjectPool<>(
				new NettyClientFactory(new InetSocketAddress("localhost", server.getPort())));
		clientPool.initialize();
		clientPool.start();
	}

	@Test
	public void testReset() throws CommandExecutionException, InterruptedException, ExecutionException, IOException {

		String request = randomString(1 << 5) + "\r\n";

		TestCommand command = new TestCommand(request, 1000, clientPool, scheduler, null);
		CommandFuture<String> future = command.execute();
		String result = future.get();
		Assert.assertEquals(request, result);

		command.reset();
		future = command.execute();
		result = future.get();
		Assert.assertEquals(request, result);
	}

	@Test
	public void testSuccess() throws Exception {

		String request = randomString() + "\r\n";
		TestCommand command = new TestCommand(request, clientPool, scheduler);
		CommandFuture<String> future = command.execute();
		String result = future.get();
		Assert.assertEquals(request, result);

		final AtomicReference<String> listenerResult = new AtomicReference<String>();
		final CountDownLatch latch = new CountDownLatch(1);
		future.addListener(new CommandFutureListener<String>() {

			@Override
			public void operationComplete(CommandFuture<String> commandFuture) throws Exception {
				try {
					if (commandFuture.isSuccess()) {
						listenerResult.set(commandFuture.get());
					} else {
						listenerResult.set(null);
					}
				} finally {
					latch.countDown();
				}

			}
		});

		latch.await(1, TimeUnit.SECONDS);
		Assert.assertEquals(request, listenerResult.get());

	}

	@Test
	public void testTimeout() throws CommandExecutionException, InterruptedException {

		TestCommand testCommand = new TestCommand("sleep 5000\r\n", 1000, clientPool, scheduler, null);
		CommandFuture<String> future = testCommand.execute();

		final AtomicReference<CommandFuture<String>> listenerFuture = new AtomicReference<CommandFuture<String>>(null);
		final CountDownLatch latch = new CountDownLatch(1);
		future.addListener(new CommandFutureListener<String>() {

			@Override
			public void operationComplete(CommandFuture<String> commandFuture) throws Exception {

				try {
					listenerFuture.set(commandFuture);
				} finally {
					latch.countDown();
				}
			}
		});

		try {
			future.get();
			Assert.fail();
		} catch (InterruptedException e) {
			Assert.fail();
		} catch (ExecutionException e) {
			if (!(e.getCause() instanceof CommandTimeoutException)) {
				Assert.fail();
			}
		}

		latch.await();
		Assert.assertTrue(listenerFuture.get() != null);

	}

	@Test
	public void testClosed() throws CommandExecutionException, BorrowObjectException {

		TestCommand testCommand = new TestCommand("something", 0, clientPool, scheduler, null);
		CommandFuture<String> future = testCommand.execute();

		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					TimeUnit.SECONDS.sleep(1);
					clientPool.borrowObject().channel().close();
				} catch (BorrowObjectException | InterruptedException e) {
					logger.error("[testClosed]", e);
				}
			}
		}).start();

		try {
			future.get();
			Assert.fail();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			if (!(e instanceof ExecutionException && (((ExecutionException) e).getCause() instanceof IOException))) {
				Assert.fail();
			}
		}

	}

	@Test
	public void testException() throws CommandExecutionException {

		Exception exception = new Exception();
		TestCommand testCommand = new TestCommand("something\r\n", 0, clientPool, scheduler, exception);
		CommandFuture<String> future = testCommand.execute();
		try {
			future.get();
			Assert.fail();
		} catch (InterruptedException e) {

			Assert.fail();
		} catch (ExecutionException e) {

			Assert.assertEquals(exception, e.getCause());
		}
	}

	@Test
	public void testSchedule() throws InterruptedException, ExecutionException, TimeoutException {

		String request = randomString() + "\r\n";

		TestCommand testCommand = new TestCommand(request, 0, clientPool, scheduler, null);
		CommandFuture<String> future = testCommand.execute(1, TimeUnit.SECONDS);

		sleep(10);
		String result = future.getNow();
		Assert.assertNull(result);

		result = future.get(2, TimeUnit.SECONDS);
		Assert.assertEquals(request, result);
	}

	@Test(expected = CancellationException.class)
	public void testScheduleCancel() throws InterruptedException, ExecutionException, TimeoutException {

		String request = randomString() + "\r\n";

		TestCommand testCommand = new TestCommand(request, 0, clientPool, scheduler, null);
		CommandFuture<String> future = testCommand.execute(1, TimeUnit.SECONDS);

		sleep(10);
		String result = future.getNow();
		Assert.assertNull(result);

		future.cancel(false);
		result = future.get(2, TimeUnit.SECONDS);
	}

	class TestCommand extends AbstractNettyRequestResponseCommand<String> {

		private String request;
		private ByteArrayOutputStream result = new ByteArrayOutputStream();
		private int timeout;
		private Exception e;

		public TestCommand(String request, XpipeObjectPool<NettyClient> clientPool,
				ScheduledExecutorService scheduled) {
			this(request, 1000, clientPool, scheduled, null);
		}

		public TestCommand(String request, int timeout, XpipeObjectPool<NettyClient> clientPool,
				ScheduledExecutorService scheduled, Exception e) {
			super(clientPool, scheduled);
			this.request = request;
			this.timeout = timeout;
			this.e = e;
		}

		@Override
		protected String doReceiveResponse(Channel channel, ByteBuf byteBuf) throws Exception {

			if (e != null) {
				throw e;
			}
			int readable = byteBuf.readableBytes();
			byte[] buff = new byte[readable];
			byteBuf.readBytes(buff);
			result.write(buff);
			logger.debug("[doReceiveResponse]{}, {}", readable, new String(buff));
			if (result.size() >= request.length()) {
				return new String(result.toByteArray());
			}
			return null;
		}

		@Override
		public int getCommandTimeoutMilli() {
			return timeout;
		}

		@Override
		protected ByteBuf getRequest() {
			return Unpooled.wrappedBuffer(request.getBytes());
		}

		@Override
		protected void doReset() {
			result.reset();
		}

		@Override
		public String getName() {
			return "unittest comamnd";
		}

	}
}
