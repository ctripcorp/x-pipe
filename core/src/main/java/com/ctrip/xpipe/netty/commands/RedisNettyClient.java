package com.ctrip.xpipe.netty.commands;

public interface RedisNettyClient {

    boolean getDoAfterConnectedOver();

    int getAfterConnectCommandTimeoutMill();

}
