package com.ctrip.xpipe.redis.core.protocal.cmd;

import java.util.concurrent.ScheduledExecutorService;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterRole;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.utils.StringUtil;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
public class RoleCommand extends AbstractRedisCommand<Role>{

	public RoleCommand(String host, int port, ScheduledExecutorService scheduled) {
		super(host, port, scheduled);
	}
	
	public RoleCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
	}

	@Override
	public String getName() {
		return "role";
	}

	@Override
	protected Role format(Object payload) {
		
		if(payload instanceof Object[]){
			Object []arrayPayload = (Object[]) payload;
			if(arrayPayload.length == 5){
				return new SlaveRole(arrayPayload);
			}else if(arrayPayload.length == 3){
				return new MasterRole(arrayPayload);
			}
			throw new IllegalStateException("unknown supported payload:" + StringUtil.join(",", arrayPayload));
		}
		throw new IllegalStateException("unknown payload:" + payload);
	}

	@Override
	public ByteBuf getRequest() {
		return new RequestStringParser("role").format();
	}
}
