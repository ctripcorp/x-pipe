package com.ctrip.xpipe.redis.console.migration.status;

import com.ctrip.xpipe.redis.console.migration.status.migration.statemachine.StateActionState;

/**
 * @author shyin
 *         <p>
 *         Dec 8, 2016
 */
public interface MigrationState {

    MigrationStatus getStatus();

    void refresh();

    MigrationState nextAfterSuccess();

    MigrationState nextAfterFail();

    boolean setStateActionState(StateActionState current, StateActionState future);

    StateActionState getStateActionState();

}
