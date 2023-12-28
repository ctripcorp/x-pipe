package com.ctrip.xpipe.redis.console.keeper.handler;

public interface Handler<T> {
    boolean handle(T t);

    Handler<T> setNextHandler(Handler<T> nextHandler);

}
