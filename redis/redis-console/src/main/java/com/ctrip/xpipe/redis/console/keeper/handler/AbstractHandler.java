package com.ctrip.xpipe.redis.console.keeper.handler;

public abstract class AbstractHandler<T> implements Handler<T>{

    private Handler<T> nextHandle;

    public Handler<T> setNextHandler(Handler<T> nextHandler) {
        this.nextHandle = nextHandler;
        return this.nextHandle;
    }

    protected abstract boolean doNextHandler(T t);

    @Override
    public boolean handle(T t) {
        if (doNextHandler(t)) {
            if (this.nextHandle != null) {
                return this.nextHandle.handle(t);
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

}
