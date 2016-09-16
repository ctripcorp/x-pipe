package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.pojo.KeeperRole;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.utils.StringUtil;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
public class RoleCommand extends AbstractRedisCommand<Role>{

	public RoleCommand(String host, int port) {
		super(host, port);
	}
	
	public RoleCommand(SimpleObjectPool<NettyClient> clientPool) {
		super(clientPool);
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
				return new KeeperRole(arrayPayload);
			}
			throw new IllegalStateException("unknown supported payload:" + StringUtil.join(",", arrayPayload));
		}
		throw new IllegalStateException("unknown payload:" + payload);
	}

	@Override
	protected ByteBuf getRequest() {
		return new SimpleStringParser("role").format();
	}
}
