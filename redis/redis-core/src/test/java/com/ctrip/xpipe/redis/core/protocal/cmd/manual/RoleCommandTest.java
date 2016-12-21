package com.ctrip.xpipe.redis.core.protocal.cmd.manual;

import org.junit.Test;

import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;



/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
public class RoleCommandTest extends AbstractCommandTest{
	
	@Test
	public void test() throws Exception{
		
		RoleCommand roleCommand = new RoleCommand("10.3.2.23", 6379, scheduled);
		Role real = roleCommand.execute().get();
		System.out.println(real.getServerRole() == SERVER_ROLE.SLAVE);
		logger.info("[test]{}", real);
	}
	
	
}
