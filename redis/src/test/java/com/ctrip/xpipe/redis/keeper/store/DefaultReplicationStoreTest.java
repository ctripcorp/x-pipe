package com.ctrip.xpipe.redis.keeper.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class DefaultReplicationStoreTest {

	@Test
	public void test() throws Exception {
		File baseDir = new File(System.getProperty("java.io.tmpdir"), "xpipe");
		baseDir.deleteOnExit();
		System.out.println(baseDir.getCanonicalFile());

		DefaultReplicationStore store = new DefaultReplicationStore(baseDir, 15);
		store.beginRdb("master", -1, -1);

		int cmdCount = 4;
		int cmdLen = 10;

		final CountDownLatch latch = new CountDownLatch(cmdCount * cmdLen);
		final StringBuffer got = new StringBuffer();
		store.addCommandsListener(0, new CommandsListener() {

			@Override
			public void onCommand(ByteBuf byteBuf) {
				int len = byteBuf.readableBytes();
				got.append(new String(byteBuf.array(), byteBuf.arrayOffset(), len));
				for (int i = 0; i < len; i++) {
					latch.countDown();
				}
			}
		});

		StringBuffer exp = new StringBuffer();
		for (int j = 0; j < cmdCount; j++) {
			ByteBuf buf = Unpooled.buffer();
			String cmd = UUID.randomUUID().toString().substring(0, cmdLen);
			exp.append(cmd);
			buf.writeBytes(cmd.getBytes());
			store.appendCommands(buf);
		}

		assertTrue(latch.await(1, TimeUnit.SECONDS));
		assertEquals(exp.toString(), got.toString());
		store.close();
	}

}
