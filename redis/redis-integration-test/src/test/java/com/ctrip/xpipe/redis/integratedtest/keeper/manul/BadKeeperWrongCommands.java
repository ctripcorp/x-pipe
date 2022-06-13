package com.ctrip.xpipe.redis.integratedtest.keeper.manul;

import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.integratedtest.keeper.AbstractKeeperIntegratedSingleDc;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import io.netty.buffer.Unpooled;
import org.apache.commons.exec.ExecuteException;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 *         Sep 29, 2016
 */
public class BadKeeperWrongCommands extends AbstractKeeperIntegratedSingleDc {

	private ReplicationStore replicationStore;

	private String[] createCommands() {

		int valueLen = 20;
		
		String setcommand = "set b ";
		setcommand += randomString(valueLen - setcommand.length());
		
		return new String[] { 
				"*3\r\n"
				+ "$3\r\nset\r\n"
				+ "$1\r\na\r\n"
				+ "$" + valueLen + "\r\n"
				, setcommand + "\r\n" };
	}

	@SuppressWarnings("unused")
	@Test
	public void giveWrongCommands() throws IOException {

		String[] commands = createCommands();

		RedisKeeperServer active = getRedisKeeperServer(activeKeeper);
		RedisKeeperServer backup = getRedisKeeperServer(backupKeeper);
		
		replicationStore = active.getReplicationStore();
		
		Set<RedisSlave<RedisKeeperServer>> slaves = active.slaves();

		
		replicationStore.appendCommands(Unpooled.wrappedBuffer("*3\r\n$3\r\nset\r\n$1\r\na\r\n$5\r\n12 34\r\n\r\n".getBytes()));
		
		logger.info(remarkableMessage("stop redis master"));
		waitForAnyKey();
		stopRedisMaster();
		
		printSlaves(slaves);

		
		logger.info(remarkableMessage("give part commands:" + commands[0].length()));
		waitForAnyKey();
		replicationStore.appendCommands(Unpooled.wrappedBuffer(commands[0].getBytes()));

		
		logger.info(remarkableMessage("close client connection"));
		waitForAnyKey();
		for (RedisSlave slave : active.slaves()) {
			slave.close();
		}

		System.out.println("give other commands");
		waitForAnyKey();
		for (int i = 1; i < commands.length; i++) {
			replicationStore.appendCommands(Unpooled.wrappedBuffer(commands[i].getBytes()));
		}

		waitForAnyKeyToExit();
	}

	private void printSlaves(Set<RedisSlave<RedisKeeperServer>> slaves) {
		
		for(RedisSlave redisSlave : slaves){
			logger.info("{}", redisSlave.info());
		}
	}

	private void stopRedisMaster() throws ExecuteException, IOException {
		stopServerListeningPort(getRedisMaster().getPort());
	}

	@Override
	protected KeeperConfig getKeeperConfig() {
		return new TestKeeperConfig(1 << 30, 5, 1 << 30, 300000);
	}

}
