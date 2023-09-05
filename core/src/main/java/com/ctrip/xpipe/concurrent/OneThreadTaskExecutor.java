package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.command.LogIgnoreCommand;
import com.ctrip.xpipe.command.RetryCommandFactory;
import com.ctrip.xpipe.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
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

    protected Queue<Command<?>> tasks;

    private AtomicBoolean isRunning = new AtomicBoolean(false);

    private AtomicBoolean destroyed = new AtomicBoolean(false);

    @SuppressWarnings("rawtypes")
    private RetryCommandFactory retryCommandFactory;

    public OneThreadTaskExecutor(Executor executors) {
        this(DefaultRetryCommandFactory.noRetryFactory(), executors);
    }

    public OneThreadTaskExecutor(Executor executors, int queueCapacity) {
        this(DefaultRetryCommandFactory.noRetryFactory(), executors, queueCapacity);
    }

    public OneThreadTaskExecutor(RetryCommandFactory<?> retryCommandFactory, Executor executors) {
        this.retryCommandFactory = retryCommandFactory;
        this.executors = executors;
        tasks = new ConcurrentLinkedQueue<>();
    }

    public OneThreadTaskExecutor(RetryCommandFactory<?> retryCommandFactory, Executor executors, int queueCapacity) {
        this.retryCommandFactory = retryCommandFactory;
        this.executors = executors;
        tasks = new LinkedBlockingQueue<>(queueCapacity);
    }

    public void executeCommand(Command<?> command) {

        logger.debug("[executeCommand][offer it in pool]{}", command);
        boolean offer = false;
        synchronized (this) {
            offer = tasks.offer(command);
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
            Command<?> command = null;
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
            logCommand(command, retryCommand);
            currentCommand = retryCommand;

            if (!(command instanceof LogIgnoreCommand)) logger.info("[doRun][begin]{}", command);
            retryCommand.future().addListener(new CommandFutureListener() {
                @Override
                public void operationComplete(CommandFuture commandFuture) throws Exception {
                    currentCommand = null;
                    if (!isRunning.compareAndSet(true, false)) {
                        logger.error("[doRun][already exit]");
                    }

                    doExecute();
                }
            });
            retryCommand.execute();
        }

    }

    protected void logCommand(Command<?> originCommand, Command<?> retryCommand) {
        retryCommand.future().addListener(new CommandFutureListener() {
            @Override
            public void operationComplete(CommandFuture commandFuture) throws Exception {
                if (originCommand instanceof LogIgnoreCommand) {
                    // do not log
                } else if (commandFuture.isSuccess()){
                    logger.info("[doRun][ end ][succeed]{}", originCommand);
                } else if (ExceptionUtils.isStackTraceUnnecessary(commandFuture.cause())) {
                    logger.error("[doRun][ end ][fail]{}, {}", originCommand, commandFuture.cause().getMessage());
                } else {
                    logger.error("[doRun][ end ][fail]" + originCommand, commandFuture.cause());
                }
            }
        });
    }

    protected Command retryCommand(Command<?> command) throws Exception {
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
