package com.ctrip.xpipe.redis.console.migration.status.migration.statemachine;

import com.ctrip.xpipe.redis.console.migration.status.ActionMigrationState;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 31, 2017
 */
public class Inited extends AbstractStateActionState {

    public Inited(ActionMigrationState migrationState) {
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
        throw new IllegalStateException("have not done tryAction yet");
    }

    @Override
    public void rollbackDone() {
        throw new IllegalStateException("have not done tryRollback yet");
    }

    @Override
    public boolean allowTimeout() {
        return false;
    }

    @Override
    public void timeout() {
        throw new IllegalStateException("time out not allowed");
    }
}
