package com.ctrip.xpipe.redis.console.controller.migrate.meta;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 22, 2017
 */
public enum  DO_STATUS {

    INITED,
    MIGRATING,
    FAIL,
    SUCCESS,
    ROLLBACK,
    ROLLBACKSUCCESS

}
