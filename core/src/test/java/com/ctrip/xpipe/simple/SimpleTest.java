package com.ctrip.xpipe.simple;


import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.dianping.cat.configuration.client.entity.ClientConfig;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public class SimpleTest extends AbstractTest{
	
	@Test
	public <V> void testCommand(){
		ClientConfig clientConfig = new ClientConfig();
		System.out.println(clientConfig);
	}
	
	@Test
	public void testThread(){
		
		Thread thread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				
				try {
					logger.info("[run][begin sleep]");
					TimeUnit.SECONDS.sleep(5);
					logger.info("[run][end   sleep]");
				} catch (InterruptedException e) {
				}
			}
		});
		
		logger.info("[testThread]{}", thread.isAlive());
		
		thread.start();
		
		sleep(1000);
		logger.info("[testThread]{}", thread.isAlive());

		sleep(6000);
		logger.info("[testThread]{}", thread.isAlive());
		
		thread.start();
		
		sleep(1000);
		logger.info("[testThread]{}", thread.isAlive());
	}
	
	@Test
	public void testNetty(){
		
		CompositeByteBuf byteBuf = ByteBufAllocator.DEFAULT.compositeBuffer();
		byteBuf.addComponent(Unpooled.wrappedBuffer("12345".getBytes()));
		byteBuf.addComponent(Unpooled.wrappedBuffer("abcde".getBytes()));

		System.out.println(ByteBufUtils.readToString(byteBuf));
		
		ByteBuf buf = Unpooled.wrappedBuffer(Unpooled.wrappedBuffer("134".getBytes()), Unpooled.wrappedBuffer("abc".getBytes()));
		System.out.println(buf.readableBytes());
		byte []result = new byte[buf.readableBytes()];
		buf.readBytes(result);
		System.out.println(new String(result));
		
	}
}
