package com.ctrip.xpipe.netty.commands;

import java.io.IOException;

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
	
}
