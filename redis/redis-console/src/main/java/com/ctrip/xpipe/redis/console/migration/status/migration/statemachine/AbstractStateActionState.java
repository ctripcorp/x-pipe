package com.ctrip.xpipe.redis.console.migration.status.migration.statemachine;

import com.ctrip.xpipe.redis.console.migration.status.ActionMigrationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 31, 2017
 */
public abstract class AbstractStateActionState implements StateActionState{

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected ActionMigrationState migrationState;

    private long startTime = System.currentTimeMillis();

    public AbstractStateActionState(ActionMigrationState migrationState){
        this.migrationState = migrationState;
    }

    protected void doAction() {
        migrationState.action();
    }

    protected void doRollback() {
        migrationState.rollback();
    }


    public boolean doSetState(StateActionState stateActionState) {

        if(migrationState.setStateActionState(this, stateActionState)){

            if(stateActionState.allowTimeout()){
                migrationState.cancelCheckTimeout();
                migrationState.checkTimeout();
            }

            if(stateActionState instanceof Done){
                migrationState.cancelCheckTimeout();
            }

            return true;
        }
        return false;
    }


    public long getStartTime() {
        return startTime;
    }

    @Override
    public void timeout() {
        logger.info("[timeout]{}, {}", migrationState, new Date(getStartTime()));
        doSetState(new Done(migrationState));
    }


    @Override
    public String toString() {
        return String.format("%s", getClass().getSimpleName());
    }
}
