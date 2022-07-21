package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.ActionMigrationState;
import com.ctrip.xpipe.redis.console.migration.status.MigrationState;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.migration.status.migration.statemachine.Inited;
import com.ctrip.xpipe.redis.console.migration.status.migration.statemachine.StateActionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author shyin
 *         <p>
 *         Dec 8, 2016
 */
public abstract class AbstractMigrationState implements ActionMigrationState {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected static final int DEFAULT_MIGRATION_WAIT_TIME_MILLI = Integer.parseInt(System.getProperty("MIGRATION_TIMEOUT_MILLI", "120000"));;

    protected int migrationWaitTimeMilli = DEFAULT_MIGRATION_WAIT_TIME_MILLI;

    private AtomicBoolean hasContine = new AtomicBoolean(false);

    private MigrationCluster holder;
    private MigrationStatus status;

    private MigrationState nextAfterSuccess;
    private MigrationState nextAfterFail;

    private AtomicReference<StateActionState> stateActionState = new AtomicReference<>();

    protected Executor executors;
    private ScheduledExecutorService scheduled;
    private ScheduledFuture<?> future;


    public AbstractMigrationState(MigrationCluster holder, MigrationStatus status) {
        this.holder = holder;
        this.status = status;
        this.executors = holder.getMigrationExecutor();
        this.scheduled = holder.getScheduled();
        this.stateActionState.set(new Inited(this));
    }

    @Override
    public StateActionState getStateActionState() {
        return stateActionState.get();
    }

    public void setMigrationWaitTimeMilli(int migrationWaitTimeMilli) {
        this.migrationWaitTimeMilli = migrationWaitTimeMilli;
    }

    @Override
    public void cancelCheckTimeout() {
        if(future != null){
            future.cancel(true);
        }
    }

    @Override
    public void checkTimeout() {

        future = scheduled.schedule(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {

                logger.info("[checkTimeout][timeout]{}", AbstractMigrationState.this);
                getStateActionState().timeout();
            }
        },migrationWaitTimeMilli, TimeUnit.MILLISECONDS);
    }

    @Override
    public void action() {
        hasContine.set(false);
        doAction();
    }

    @Override
    public void rollback() {
        try{
            logger.info("[tryRollback]{}", this);
            doRollback();
        }finally {
            markRollbackDone();
        }
    }

    protected void markDone(){
        this.stateActionState.get().actionDone();
    }

    protected void markRollbackDone(){
        this.stateActionState.get().rollbackDone();
    }

    @Override
    public boolean setStateActionState(StateActionState current, StateActionState future){

        StateActionState previous = this.stateActionState.get();
        if(this.stateActionState.compareAndSet(current, future)){
            logger.info("[setStateActionState][success]{}, {}->{}", this, previous, stateActionState);
            return true;
        }else{
            logger.info("[setStateActionState][fail]{}, {}, {}->{}", this, previous, current, stateActionState);
            return false;
        }
    }

    protected abstract void doRollback();

    protected abstract void doAction();

    public MigrationCluster getHolder() {
        return holder;
    }

    @Override
    public MigrationStatus getStatus() {
        return status;
    }

    @Override
    public MigrationState nextAfterSuccess() {
        return nextAfterSuccess;
    }

    public AbstractMigrationState setNextAfterSuccess(MigrationState nextAfterSuccess) {
        this.nextAfterSuccess = nextAfterSuccess;
        return this;
    }

    @Override
    public MigrationState nextAfterFail() {
        return nextAfterFail;
    }

    public AbstractMigrationState setNextAfterFail(MigrationState nextAfterFail) {
        this.nextAfterFail = nextAfterFail;
        return this;
    }

    protected void updateAndProcess(MigrationState state) {
        markDone();
        updateAndProcess(state, true, false);
    }

    protected void updateAndForceProcess(MigrationState state) {
        try {
            markDone();
        } catch (Throwable th) {
            logger.info("[updateAndForceProcess] ignore mark done fail", th);
        }

        updateAndProcess(state, true, true);
    }

    protected void rollbackToState(MigrationState state) {

        updateAndProcess(state, true, true);
    }

    protected void updateAndStop(MigrationState state) {
        markDone();
        updateAndProcess(state, false, false);
        getHolder().update(getHolder(), getHolder()); // notify for migration paused
    }

    private void updateAndProcess(MigrationState stat, boolean process, boolean forceContinue) {

        if (forceContinue || hasContine.compareAndSet(false, true)) {
            logger.info("[updateAndProcess][continue]{}, {}, {}", getHolder().clusterName(), stat, process);
            try {
                getHolder().updateStat(stat);
            } catch (Throwable th) {
                logger.info("[updateAndProcess]{} update stat fail and stop", getHolder().clusterName(), th);
                throw th;
            }

            if (process) {
                getHolder().process();
            }
        } else {
            logger.info("[updateAndProcess][already continue]{}, {}, {}", getHolder().clusterName(), stat, process);
        }
    }

    @Override
    public String toString() {
        return String.format("%s:%s", getClass().getSimpleName(), getHolder() != null ? getHolder().clusterName() : "");
    }
}
