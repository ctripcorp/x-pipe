package com.ctrip.xpipe.redis.core.protocal.pojo;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 07, 2017
 */
public abstract class AbstractInfo implements RedisInfo{

    private static final String KEEPER_ROLE_PREFIX = RedisProtocol.KEEPER_ROLE_PREFIX;

    private Server.SERVER_ROLE serverRole;

    private boolean isKeeper;

    public AbstractInfo(){

    }
    public AbstractInfo(Server.SERVER_ROLE serverRole, boolean isKeeper){
        this.serverRole = serverRole;
        this.isKeeper = isKeeper;
    }

    @Override
    public Server.SERVER_ROLE getRole() {
        return serverRole;
    }

    @Override
    public boolean isKeeper() {
        return isKeeper;
    }


    protected static boolean getField(String key, String value, AbstractInfo info) {
        if(key.equalsIgnoreCase(KEEPER_ROLE_PREFIX)){
            info.isKeeper = true;
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return JsonCodec.INSTANCE.encode(this);
    }

}
