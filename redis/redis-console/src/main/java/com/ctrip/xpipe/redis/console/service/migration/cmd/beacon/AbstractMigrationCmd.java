package com.ctrip.xpipe.redis.console.service.migration.cmd.beacon;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;

/**
 * @author lishanglin
 * date 2021/4/29
 */
public abstract class AbstractMigrationCmd<T> extends AbstractCommand<T> {

    protected static final String MIGRATION_OPERATOR = "Beacon";

    protected static final String TYPE = "Migration.Beacon";

    private BeaconMigrationRequest migrationRequest;

    public AbstractMigrationCmd(BeaconMigrationRequest migrationRequest) {
        this.migrationRequest = migrationRequest;
    }

    @Override
    protected void doExecute() throws Throwable {
        Transaction transaction = Cat.newTransaction(TYPE, getClass().getSimpleName());
        try {
            transaction.addData("cluster", migrationRequest.getClusterName());
            transaction.addData("forced", migrationRequest.getIsForced());
            transaction.addData("targetIdc", migrationRequest.getTargetIDC());
            innerExecute();
            transaction.setStatus(Message.SUCCESS);
        } catch (Throwable th) {
            transaction.setStatus(th);
            throw th;
        } finally {
            transaction.complete();
        }
    }

    protected abstract void innerExecute() throws Throwable;

    protected BeaconMigrationRequest getMigrationRequest() {
        return migrationRequest;
    }

    @Override
    protected void doReset() {
        // do nothing
    }

    @Override
    public String getName() {
        return String.format("%s-%s", getClass().getSimpleName(), getMigrationRequest().getClusterName());
    }

}
