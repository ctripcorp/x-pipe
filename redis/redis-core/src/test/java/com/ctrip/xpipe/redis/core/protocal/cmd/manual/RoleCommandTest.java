package com.ctrip.xpipe.redis.core.protocal.cmd.manual;

import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
public class RoleCommandTest extends AbstractCommandTest{
	
	@Test
	public void testMaster() throws Exception{
		
		Server master = startServer("*3\r\n"
				+ "$6\r\nmaster\r\n"
				+ ":43\r\n"
				+ "*3\r\n"
					+ "$9\r\n127.0.0.1\r\n"
					+ "$4\r\n6479\r\n"
					+ "$1\r\n0\r\n");
		RoleCommand roleCommand = new RoleCommand(
				getXpipeNettyClientKeyedObjectPool().getKeyPool(localhostInetAddress(master.getPort())),
				2000,
				false,
				scheduled);
		Role role = roleCommand.execute().get();
		
		Assert.assertEquals(SERVER_ROLE.MASTER, role.getServerRole());
		
	}
	
	@Test
	public void testSlave() throws Exception{

		for(MASTER_STATE masterState : MASTER_STATE.values()){
			
			Server slave = startServer("*5\r\n"
					+ "$5\r\nslave\r\n"
					+ "$9\r\nlocalhost\r\n"
					+ ":6379\r\n"
					+ "$" +masterState.getDesc().length()+ "\r\n" + masterState.getDesc()+ "\r\n"
					+ ":477\r\n");
			RoleCommand roleCommand = new RoleCommand(
					getXpipeNettyClientKeyedObjectPool().getKeyPool(localhostInetAddress(slave.getPort())),
					2000, false, scheduled);
			SlaveRole role = (SlaveRole) roleCommand.execute().get();
			
			Assert.assertEquals(SERVER_ROLE.SLAVE, role.getServerRole());
			Assert.assertEquals("localhost", role.getMasterHost());
			Assert.assertEquals(6379, role.getMasterPort());
			Assert.assertEquals(masterState, role.getMasterState());
			Assert.assertEquals(477, role.getMasterOffset());
		}
		
	}
	
	@Test
	public void testKeeper() throws Exception{

		Server slave = startServer("*5\r\n"
				+ "$6\r\nkeeper\r\n"
				+ "$9\r\nlocalhost\r\n"
				+ ":6379\r\n"
				+ "$9\r\nconnected\r\n"
				+ ":477\r\n");
		RoleCommand roleCommand = new RoleCommand(
				getXpipeNettyClientKeyedObjectPool().getKeyPool(localhostInetAddress(slave.getPort())),
				2000, false, scheduled);
		SlaveRole role = (SlaveRole) roleCommand.execute().get();
		
		Assert.assertEquals(SERVER_ROLE.KEEPER, role.getServerRole());
		Assert.assertEquals("localhost", role.getMasterHost());
		Assert.assertEquals(6379, role.getMasterPort());
		Assert.assertEquals(MASTER_STATE.REDIS_REPL_CONNECTED, role.getMasterState());
		Assert.assertEquals(477, role.getMasterOffset());

	}
	
	
}
