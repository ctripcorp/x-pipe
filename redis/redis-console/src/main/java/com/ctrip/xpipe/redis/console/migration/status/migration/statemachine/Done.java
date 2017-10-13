package com.ctrip.xpipe.redis.console.migration.status.migration.statemachine;

import com.ctrip.xpipe.redis.console.migration.status.ActionMigrationState;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 31, 2017
 */
public class Done extends AbstractStateActionState{

    public Done(ActionMigrationState migrationState) {
        super(migrationState);
    }

    @Override
    public void tryAction() {
        if (doSetState(new Doing(migrationState))) {
            doAction();
        }
    }

    @Override
    public void tryRollback() {
        if (doSetState(new Rollbacking(migrationState))) {
            doRollback();
        }
    }

    @Override
    public void actionDone() {
        logger.debug("[actionDone][already done]{}", migrationState);

    }

    @Override
    public void rollbackDone() {
        throw new IllegalStateException("done state, rollback done not accepted");
    }

    @Override
    public boolean allowTimeout() {
        return false;
    }
}
