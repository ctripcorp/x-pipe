package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 22, 2017
 */
public enum CHECK_FAIL_STATUS {

    MIGRATION_SYSTEM_UNHEALTHY,
    CLUSTER_NOT_FOUND,
    ACTIVE_DC_ALREADY_NOT_REQUESTED,
    ALREADY_MIGRATING,
    OTHERS
}
