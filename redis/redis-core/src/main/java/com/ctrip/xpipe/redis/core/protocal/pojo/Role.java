package com.ctrip.xpipe.redis.core.protocal.pojo;

import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 *         Sep 16, 2016
 */
public interface Role {

	SERVER_ROLE getServerRole();

	ByteBuf format();

}
