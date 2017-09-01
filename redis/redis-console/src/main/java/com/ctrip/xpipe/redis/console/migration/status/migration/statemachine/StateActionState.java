package com.ctrip.xpipe.redis.console.migration.status.migration.statemachine;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 31, 2017
 */
public interface StateActionState {

    void tryAction();

    void tryRollback();

    void actionDone();

    void rollbackDone();

    boolean allowTimeout();

    void timeout();
}
