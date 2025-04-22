package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.command.AbstractCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 12:30
 */
public class SuccessSequenceCommand extends SequenceCommand<Boolean> {

    private static final Logger staticLogger = LoggerFactory.getLogger(SuccessSequenceCommand.class);

    private static class SuccessCommand extends AbstractCommand<Boolean> {

        @Override
        public String getName() {
            return "SuccessCommand";
        }

        @Override
        protected void doExecute() throws Throwable {
            future().setSuccess(true);
        }

        @Override
        protected void doReset() {

        }
    }

    public SuccessSequenceCommand(List<SequenceCommand<?>> pasts, Executor stateThread, Executor workerThreads) {
        super(pasts, new SuccessCommand(), stateThread, workerThreads);
    }

    @Override
    protected Logger getLogger() {
        return staticLogger;
    }
}
