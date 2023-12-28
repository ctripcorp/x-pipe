package com.ctrip.xpipe.redis.console.keeper.handler;

public abstract class AbstractHandler<T> implements Handler<T>{

    private Handler<T> nextHandler;

    public Handler<T> setNextHandler(Handler<T> nextHandler) {
        this.nextHandler = nextHandler;
        return this;
    }

    protected abstract boolean doNextHandler(T t);

    @Override
    public boolean handle(T t) {
        if (doNextHandler(t)) {
            if (nextHandler != null) {
                return nextHandler.handle(t);
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

}
