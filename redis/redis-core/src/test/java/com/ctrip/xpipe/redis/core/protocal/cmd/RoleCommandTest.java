package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
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
public class RoleCommandTest extends AbstractRedisTest{
	
	@Test
	public void test() throws Exception{
		
		SlaveRole role = new SlaveRole(SERVER_ROLE.KEEPER, "localhost", randomPort(), MASTER_STATE.REDIS_REPL_CONNECT, 0L);
		
		Server server =  startServer(ByteBufUtils.readToString(role.format()));
		RoleCommand roleCommand = new RoleCommand("localhost", server.getPort(), scheduled);
		
		Role real = roleCommand.execute().get();

		logger.info("[test]{}", real);
		Assert.assertEquals(role, real);
	}
	
	
}
