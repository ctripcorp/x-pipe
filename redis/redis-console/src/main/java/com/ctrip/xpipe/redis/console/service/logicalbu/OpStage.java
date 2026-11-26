package com.ctrip.xpipe.redis.console.service.logicalbu;

/** Step of a keeper-ops run, in execution order. */
public enum OpStage {
    /** read logical BU / keepercontainers / dc names */
    LOAD,
    UPDATE_DB,
    NOTIFY_META,
    /** reset health: list keepers of each TFS keepercontainer, then /keepers/health/recover each one */
    RECOVER_HEALTH
}
