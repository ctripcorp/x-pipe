package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.command.LogCommandWrapper;
import com.ctrip.xpipe.command.RetryCommandFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 7, 2016
 */
public class OneThreadTaskExecutor implements Destroyable {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private Executor executors;

    private Command<?> currentCommand;

    protected Queue<LogCommandWrapper<?>> tasks = new ConcurrentLinkedQueue<>();

    private AtomicBoolean isRunning = new AtomicBoolean(false);

    private AtomicBoolean destroyed = new AtomicBoolean(false);

    @SuppressWarnings("rawtypes")
    private RetryCommandFactory retryCommandFactory;

    public OneThreadTaskExecutor(Executor executors) {
        this(DefaultRetryCommandFactory.noRetryFactory(), executors);
    }

    public OneThreadTaskExecutor(RetryCommandFactory<?> retryCommandFactory, Executor executors) {
        this.retryCommandFactory = retryCommandFactory;
        this.executors = executors;
    }

    public void executeCommand(Command<?> command) {
        executeCommand(command, true);
    }

    public void executeCommand(Command<?> command, boolean needLog) {

        logger.debug("[executeCommand][offer it in pool]{}", command);
        boolean offer = false;
        synchronized (this) {
            offer = tasks.offer(new LogCommandWrapper<>(command, needLog));
        }
        if (!offer) {
            throw new IllegalStateException("pool full:" + tasks.size());
        }
        doExecute();
    }

    private void doExecute() {
        executors.execute(new Task());
    }


    public class Task extends AbstractExceptionLogTask {

        protected Logger logger = OneThreadTaskExecutor.this.logger;
        @Override
        @SuppressWarnings({"unchecked"})
        protected void doRun() throws Exception {

            if (!isRunning.compareAndSet(false, true)) {
                logger.debug("[doRun][already run]{}", this);
                return;
            }
            LogCommandWrapper<?> command = null;
            synchronized (this) {
                command = tasks.poll();
            }
            if (command == null) {
                isRunning.compareAndSet(true, false);
                logger.debug("[no command][exit]{}", OneThreadTaskExecutor.this);
                return;
            }

            if (destroyed.get()) {
                isRunning.compareAndSet(true, false);
                logger.info("[destroyed][exit]{}", OneThreadTaskExecutor.this);
                return;
            }

            Command retryCommand = retryCommand(command);
            final boolean needLog = command.isNeedLog();
            currentCommand = retryCommand;

            if (needLog) logger.info("[doRun][begin]{}", command);
            Command<?> finalCommand = command;
            retryCommand.future().addListener(new CommandFutureListener() {
                @Override
                public void operationComplete(CommandFuture commandFuture) throws Exception {
                    currentCommand = null;
                    if (!isRunning.compareAndSet(true, false)) {
                        logger.error("[doRun][already exit]");
                    }

                    if (!needLog) {
                        // do not log
                    } else if (commandFuture.isSuccess()){
                        logger.info("[doRun][ end ][succeed]{}", finalCommand);
                    }else {
                        logger.error("[doRun][ end ][fail]" + finalCommand, commandFuture.cause());
                    }
                    doExecute();
                }
            });
            retryCommand.execute();
        }

    }

    protected Command retryCommand(LogCommandWrapper<?> command) throws Exception {
        return retryCommandFactory.createRetryCommand(command);

    }

    protected Command<?> getCurrentCommand() {
        return currentCommand;
    }

    @Override
    public void destroy() throws Exception {
        logger.info("[destroy]{}", this);
        destroyed.set(true);
    }
}
