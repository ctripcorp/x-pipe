package com.ctrip.xpipe.redis.console.migration.status;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 27, 2017
 */
public interface PublishState extends MigrationState{

    void forceEnd();

}
