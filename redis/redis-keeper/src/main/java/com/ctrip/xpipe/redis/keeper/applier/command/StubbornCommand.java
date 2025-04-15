package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.keeper.applier.ApplierStatistic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Slight
 * <p>
 * Feb 07, 2022 9:50 PM
 */
public class StubbornCommand<V> extends AbstractCommand<V> implements Command<V> {

    private static final Logger logger = LoggerFactory.getLogger(StubbornCommand.class);

    private final Command<V> inner;

    private final ScheduledExecutorService retryExecutor;

    private int retryTimes;

    private ApplierStatistic statistic;

    public StubbornCommand(Command<V> inner, ScheduledExecutorService retryExecutor) {
        this(inner, retryExecutor, 180 /* 6 min */);
    }

    public StubbornCommand(Command<V> inner, ScheduledExecutorService retryExecutor, int retryTimes) {
        this.inner = inner;
        this.retryExecutor = retryExecutor;
        this.retryTimes = retryTimes;
    }

    public void setStatistic(ApplierStatistic statistic) {
        this.statistic = statistic;
    }

    @Override
    public String getName() {
        return "stubborn: " + inner.getName();
    }

    private void executeTilSuccess() {
        CommandFuture<V> future = inner.execute();

        future.addListener((f)->{
            if (f.isSuccess()) {
                try {
                    if (null != statistic) statistic.incrTrans();
                    future().setSuccess(f.get());
                } catch (Exception unlikely) {
                    logger.error("UNLIKELY - setSuccess", unlikely);
                }
            } else {
                retryTimes --;
                if (retryTimes < 0) {
                    logger.error("[{}] failed, retry too many times, stop retrying..", this, f.cause());
                    EventMonitor.DEFAULT.logAlertEvent("drop command: " + this);
                    if (null != statistic) statistic.incrDropped();

                    try {
                        future().setSuccess(null);
                    } catch (Exception unlikely) {
                        logger.error("UNLIKELY - setSuccess", unlikely);
                    }
                    return;
                }

                logger.warn("[{}] failed, retry", this, f.cause());
                inner.reset();
                retryExecutor.schedule(this::executeTilSuccess, 100, TimeUnit.MILLISECONDS);
            }
        });
    }

    @Override
    protected void doExecute() throws Throwable {
        executeTilSuccess();
    }

    @Override
    protected void doReset() {

    }
}
