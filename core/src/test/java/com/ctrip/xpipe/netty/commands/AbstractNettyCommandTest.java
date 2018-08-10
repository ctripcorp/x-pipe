package com.ctrip.xpipe.netty.commands;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * Nov 11, 2016
 */
public class AbstractNettyCommandTest extends AbstractTest{
	
	
	@Test
	public void testCommandOutOfMemory() throws IOException{
		
		TestNettyCommand command = new TestNettyCommand("localhost", randomPort());
		for(int i=0; i< (1<< 20); i++){
			
			command.execute();
			command.reset();
		}
	}

	@Test
	public void testNettyClientReturnable() throws Exception {
		Server server = startEmptyServer();
		AtomicReference<NettyClient> clientReference = new AtomicReference<>();

		SimpleObjectPool<NettyClient> clientPool = getXpipeNettyClientKeyedObjectPool().
				getKeyPool(new DefaultEndPoint("localhost", server.getPort()));

		int N = 5;
		for(int i = 0; i < N; i++) {
			AbstractNettyCommand<Void> command = new TestNettyClientReturnableCommand(clientPool, clientReference,
					(nettyClient, reference)->Assert.assertNotEquals(nettyClient, reference.getAndSet(nettyClient)));
			command.execute();
		}

		server.stop();
	}

	@Test
	public void testNettyClientReturnAfterDone() throws Exception {
		Server server = startEmptyServer();
		AtomicReference<NettyClient> clientReference = new AtomicReference<>();

		SimpleObjectPool<NettyClient> clientPool = getXpipeNettyClientKeyedObjectPool().
				getKeyPool(new DefaultEndPoint("localhost", server.getPort()));

		int N = 5;
		for(int i = 0; i < N; i++) {
			AbstractNettyCommand<Void> command = new TestNettyClientReturnableCommand(clientPool, clientReference,
					(nettyClient, reference)->{
						NettyClient oldOne = reference.getAndSet(nettyClient);
						if(oldOne != null) {
							Assert.assertEquals(nettyClient, oldOne);
						}
			});
			command.execute();
			command.future().setSuccess();
		}

		server.stop();
	}
	
	class TestNettyCommand extends AbstractNettyCommand<Void>{

		public TestNettyCommand(String host, int port) {
			super(host, port);
		}

		@Override
		public String getName() {
			return "TestNettyCommand";
		}

		@Override
		protected void doSendRequest(NettyClient nettyClient, ByteBuf byteBuf) {
			
		}

		@Override
		public ByteBuf getRequest() {
			return null;
		}

	}

	class TestNettyClientReturnableCommand extends AbstractNettyCommand<Void> {

		private AtomicReference<NettyClient> clientReference;

		private BiConsumer<NettyClient, AtomicReference<NettyClient>> consumer;

		public TestNettyClientReturnableCommand(SimpleObjectPool<NettyClient> clientPool,
												AtomicReference<NettyClient> clientReference,
												BiConsumer<NettyClient, AtomicReference<NettyClient>> consumer) {
			super(clientPool);
			this.clientReference = clientReference;
			this.consumer = consumer;
		}

		@Override
		protected void doSendRequest(NettyClient nettyClient, ByteBuf byteBuf) {
			consumer.accept(nettyClient, clientReference);
		}

		@Override
		public ByteBuf getRequest() {
			return null;
		}

		@Override
		public String getName() {
			return null;
		}

		@Override
		protected boolean returnNettyClientAfterCommand() {
			return false;
		}
	}
	
}
