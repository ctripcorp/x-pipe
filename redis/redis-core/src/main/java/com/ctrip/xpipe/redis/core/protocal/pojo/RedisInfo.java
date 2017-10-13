package com.ctrip.xpipe.redis.core.protocal.pojo;


import com.ctrip.xpipe.api.server.Server;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 08, 2017
 */
public interface RedisInfo {

    Server.SERVER_ROLE getRole();

    boolean isKeeper();

}
