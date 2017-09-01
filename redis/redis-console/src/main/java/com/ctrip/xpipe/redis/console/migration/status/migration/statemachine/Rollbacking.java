package com.ctrip.xpipe.redis.console.migration.status.migration.statemachine;

import com.ctrip.xpipe.redis.console.migration.status.ActionMigrationState;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 31, 2017
 */
public class Rollbacking extends AbstractStateActionState{

    public Rollbacking(ActionMigrationState migrationState) {
        super(migrationState);
    }

    @Override
    public void tryAction() {
        throw new IllegalStateException("rollbacking, tryAction not allowed!");
    }

    @Override
    public void tryRollback() {
        logger.info("[tryRollback][already rollbacking]{}", migrationState);
    }

    @Override
    public void actionDone() {
        throw new IllegalStateException("rollbacking, tryAction done not allowed!");
    }

    @Override
    public void rollbackDone() {
        doSetState(new Done(migrationState));
    }

    @Override
    public boolean allowTimeout() {
        return true;
    }

}
