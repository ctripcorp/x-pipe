package com.ctrip.xpipe.netty.commands;

public interface RedisNettyClient {

    boolean getDoAfterConnectedOver();

    boolean getDoAfterConnectedSuccess();

    int getAfterConnectCommandTimeoutMill();

}
