package com.ctrip.xpipe.redis.console.migration.status.migration.statemachine;

import com.ctrip.xpipe.redis.console.migration.status.ActionMigrationState;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 31, 2017
 */
public class Doing extends AbstractStateActionState{

    public Doing(ActionMigrationState migrationState) {
        super(migrationState);
    }

    @Override
    public void tryAction() {
        logger.info("[tryAction][already doing]{}", migrationState);
    }

    @Override
    public void tryRollback() {
        if(doSetState(new Rollbacking(migrationState))){
            migrationState.rollback();
        }
    }

    @Override
    public void actionDone() {
        doSetState(new Done(migrationState));
    }

    @Override
    public void rollbackDone() {
        throw new IllegalStateException("current state do not accept rollbackDone!");

    }

    @Override
    public boolean allowTimeout() {
        return true;
    }

}
