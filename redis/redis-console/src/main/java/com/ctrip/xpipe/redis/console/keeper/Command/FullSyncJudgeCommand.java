package com.ctrip.xpipe.redis.console.keeper.Command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.ObjectPoolException;
import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.command.RetryCommandFactory;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;

import java.util.concurrent.ScheduledExecutorService;

public class FullSyncJudgeCommand<T> extends AbstractKeeperCommand<T> {

    private Endpoint active;

    private Endpoint backUp;

    private long intervalTime;

    private long activeMasterReplOffset;

    private long backupMasterReplOffset;

    public FullSyncJudgeCommand(XpipeNettyClientKeyedObjectPool keyedObjectPool, ScheduledExecutorService scheduled, Endpoint active, Endpoint backUp, long intervalTime) {
        super(keyedObjectPool, scheduled);
        this.active = active;
        this.backUp = backUp;
        this.intervalTime = intervalTime;
    }

    @Override
    public String getName() {
        return "FullSyncJudgeCommand";
    }

    @Override
    protected void doExecute() throws Throwable {
        try {
            RetryCommandFactory<String> commandFactory = DefaultRetryCommandFactory.retryNTimes(scheduled, 600, 1000);
            Command<String> activeRetryInfoCommand = commandFactory.createRetryCommand(generteInfoCommand(active));
            Command<String> backUpRetryInfoCommand = commandFactory.createRetryCommand(generteInfoCommand(backUp));
            addHookAndExecute(activeRetryInfoCommand, new Callbackable<String>() {
                @Override
                public void success(String message) {
                    activeMasterReplOffset = new InfoResultExtractor(message).getMasterReplOffset();
                }

                @Override
                public void fail(Throwable throwable) {
                    logger.error("[doExecute] info instance {}:{} failed", active.getHost(), active.getPort(), throwable);
                }
            });

            try {
                Thread.sleep(intervalTime);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            addHookAndExecute(backUpRetryInfoCommand, new Callbackable<String>() {
                @Override
                public void success(String message) {
                    backupMasterReplOffset = new InfoResultExtractor(message).getMasterReplOffset();
                }

                @Override
                public void fail(Throwable throwable) {
                    logger.error("[doExecute] info instance {}:{} failed", backUp.getHost(), backUp.getPort(), throwable);
                }
            });

            if (backupMasterReplOffset != 0 && activeMasterReplOffset != 0 && backupMasterReplOffset > activeMasterReplOffset) {
                this.future().setSuccess();
            }
        } finally {
            try {
                keyedObjectPool.clear(active);
                keyedObjectPool.clear(backUp);
            } catch (ObjectPoolException e) {
                logger.error("[clear] clear keyed object pool error, activeInstance:{}, backUpInstance:{}", active, backUp, e);
            }
        }
    }

    @Override
    protected void doReset() {

    }



}
