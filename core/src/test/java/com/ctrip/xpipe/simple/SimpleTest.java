package com.ctrip.xpipe.simple;


import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.dianping.cat.configuration.client.entity.ClientConfig;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public class SimpleTest extends AbstractTest{

	@Test
	public void testCountDollar(){
		String str = "*26\r\n" +
				"$4\r\nname\r\n" + "$%d\r\n%s\r\n" +
				"$2\r\nip\r\n" + "$%d\r\n%s\r\n" +
				"$4\r\nport\r\n" + "$%d\r\n%s\r\n" +
				"$5\r\nrunid\r\n" + "$40\r\nc6831f23150c7bcb28a86534ae1f55a4a3b9068e\r\n" + "$5\r\nflags\r\n$" + "8\r\nsentinel\r\n" +
				"$16\r\npending-commands\r\n" + "$1\r\n0\r\n" + "$14\r\nlast-ping-sent\r\n" + "$1\r\n0\r\n" +
				"$18\r\nlast-ok-ping-reply\r\n" + "$3\r\n542\r\n" + "$15\r\nlast-ping-reply\r\n" + "$3\r\n542\r\n" +
				"$23\r\ndown-after-milliseconds\r\n" + "$5\r\n30000\r\n" + "$18\r\nlast-hello-message\r\n" + "$3\r\n203\r\n" +
				"$12\r\nvoted-leader\r\n" + "$1\r\n?\r\n" + "$18\r\nvoted-leader-epoch\r\n" + "$1\r\n0\r\n";

		int index = 0;
		int count = 0;
		while(true) {
			int currentIndex = str.indexOf("$", index);
			if(currentIndex < 0){
				break;
			}
			count++;
			index = currentIndex+1;
		}
		logger.info("{}", count);
	}

	@Test
	public void testInterruptSleep(){

	    executors.execute(new Runnable() {
            @Override
            public void run() {

                try {
                    logger.info("[begin sleep]");
                    TimeUnit.SECONDS.sleep(100);
                    logger.info("[end sleep]");
                } catch (InterruptedException e) {
                    logger.info("[end sleep exception]", e);
                    Thread.currentThread().interrupt();
                }
                System.out.println(Thread.currentThread().isInterrupted());
            }
        });

	    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                logger.info("hook started");
                executors.shutdownNow();
                sleep(10000);
                logger.info("hook end");
            }
        }));

	    sleep(1000000);
	}


    @Before
	public void beforeSimpleTest2(){
		System.out.println("before2");

	}

	@Before
	public void beforeSimpleTest1(){
		System.out.println("before1");
	}



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
