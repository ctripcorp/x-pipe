package com.ctrip.xpipe.redis.ctrip.integratedtest.console;

/**
 * @author lishanglin
 * date 2021/4/21
 */
@FunctionalInterface
public interface ConfContentSupplier {
    String get() throws Exception;
}
