package com.ctrip.xpipe.redis.console.migration.model;

/**
 * @author lishanglin
 * date 2020/12/14
 */
public interface MigrationLock {

    void updateLock();

    void releaseLock();

}
