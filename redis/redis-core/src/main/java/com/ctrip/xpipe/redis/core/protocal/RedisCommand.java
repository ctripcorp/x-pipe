package com.ctrip.xpipe.redis.core.protocal;

import com.ctrip.xpipe.api.command.Command;
import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * Dec 8, 2016
 */
public interface RedisCommand<T> extends Command<T>{
	
	ByteBuf getRequest();

}
