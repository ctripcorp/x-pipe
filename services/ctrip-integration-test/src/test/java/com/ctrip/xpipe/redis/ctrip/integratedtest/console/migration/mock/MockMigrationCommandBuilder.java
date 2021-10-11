package com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration.mock;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.migration.command.MigrationCommandBuilder;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author lishanglin
 * date 2021/4/22
 */
public class MockMigrationCommandBuilder implements MigrationCommandBuilder {

    public int checkDelay;

    public int prevDcDelay;

    public int newDcDelay;

    public int otherDcDelay;

    public MockMigrationCommandBuilder() {
        this(2000, 2000, 100, 2000);
    }

    public MockMigrationCommandBuilder(int checkDelay, int prevDcDelay, int newDcDelay, int otherDcDelay) {
        this.checkDelay = checkDelay;
        this.prevDcDelay = prevDcDelay;
        this.newDcDelay = newDcDelay;
        this.otherDcDelay = otherDcDelay;
    }

    @Override
    public Command<MetaServerConsoleService.PrimaryDcCheckMessage> buildDcCheckCommand(String cluster, String shard, String dc, String newPrimaryDc) {
        MetaServerConsoleService.PrimaryDcCheckMessage resp = new MetaServerConsoleService.PrimaryDcCheckMessage(MetaServerConsoleService.PRIMARY_DC_CHECK_RESULT.SUCCESS);
        return new DelayCommand<>(checkDelay, resp);
    }

    @Override
    public Command<MetaServerConsoleService.PreviousPrimaryDcMessage> buildPrevPrimaryDcCommand(String cluster, String shard, String prevPrimaryDc) {
        MetaServerConsoleService.PreviousPrimaryDcMessage resp = new MetaServerConsoleService.PreviousPrimaryDcMessage(new HostPort("127.0.0.1", 6379), null, "Prev success");
        return new DelayCommand<>(prevDcDelay, resp);
    }

    @Override
    public Command<MetaServerConsoleService.PrimaryDcChangeMessage> buildNewPrimaryDcCommand(String cluster, String shard, String newPrimaryDc, MetaServerConsoleService.PreviousPrimaryDcMessage previousPrimaryDcMessage) {
        MetaServerConsoleService.PrimaryDcChangeMessage resp = new MetaServerConsoleService.PrimaryDcChangeMessage("New success", "127.0.0.1", 6379);
        return new DelayCommand<>(newDcDelay, resp);
    }

    @Override
    public Command<MetaServerConsoleService.PrimaryDcChangeMessage> buildOtherDcCommand(String cluster, String shard, String primaryDc, String executeDc) {
        MetaServerConsoleService.PrimaryDcChangeMessage resp = new MetaServerConsoleService.PrimaryDcChangeMessage(MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT.SUCCESS, "Other success");
        return new DelayCommand<>(otherDcDelay, resp);
    }

    @Override
    public Command<MetaServerConsoleService.PreviousPrimaryDcMessage> buildRollBackCommand(String cluster, String shard, String prevPrimaryDc) {
        return null;
    }

    private static class DelayCommand<T> extends AbstractCommand<T> {

        private int delayMilli;

        private T resp;

        private static final Logger logger = LoggerFactory.getLogger(DelayCommand.class);

        public DelayCommand(int delayMilli, T resp) {
            this.delayMilli = delayMilli;
            this.resp = resp;
        }

        @Override
        protected void doExecute() throws Throwable {
            try {
                if (delayMilli > 0) TimeUnit.MILLISECONDS.sleep(delayMilli);
            } catch (Exception e) {
                logger.info("[sleep] fail", e);
            }

            future().setSuccess(resp);
        }

        @Override
        protected void doReset() {
        }

        @Override
        public String getName() {
            return "DelayCommand";
        }
    }

}
