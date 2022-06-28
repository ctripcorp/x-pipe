package com.ctrip.xpipe.redis.core.protocal.error;

/**
 * @author Slight
 * <p>
 * Jun 28, 2022 20:28
 */
public class ProxyError extends RedisError {

    public ProxyError(String message) {
        super("PROXY THROWABLE " + message);
    }

    public String errorMessage(){
        return super.getMessage();
    }
}
