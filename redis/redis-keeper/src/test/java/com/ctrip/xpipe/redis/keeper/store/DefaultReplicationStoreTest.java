package com.ctrip.xpipe.redis.keeper.store;


import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.UUID;

import org.junit.Test;

import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class DefaultReplicationStoreTest extends AbstractRedisKeeperTest{

	@Test
	public void test() throws Exception {
		
		File baseDir = new File(getTestFileDir());
		StringBuffer exp = new StringBuffer();

		int cmdCount = 4;
		int cmdLen = 10;

		DefaultReplicationStore store = new DefaultReplicationStore(baseDir, new DefaultKeeperConfig(), randomKeeperRunid(), createkeeperMonitorManager());
		store.beginRdb("master", -1, new LenEofType(-1));

		for (int j = 0; j < cmdCount; j++) {
			ByteBuf buf = Unpooled.buffer();
			String cmd = UUID.randomUUID().toString().substring(0, cmdLen);
			exp.append(cmd);
			buf.writeBytes(cmd.getBytes());
			store.getCommandStore().appendCommands(buf);
		}

		
		String result = readCommandFileTilEnd(store);

		assertEquals(exp.toString(), result);
		store.close();
	}

}
