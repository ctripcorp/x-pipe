package com.ctrip.xpipe.redis.meta.server.job.manager;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.TaskExecutor;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.util.Map;
import java.util.Objects;

/**
 * @author chen.zhu
 * <p>
 * Apr 28, 2020
 */
public abstract class AbstractDedupJobManager implements JobManager<Command<?>> {

    protected Map<Class<?>, DedupCommand> jobs = new ConcurrentHashMap<>();

    protected TaskExecutor executors;

    public AbstractDedupJobManager(TaskExecutor executors) {
        this.executors = executors;
    }

    @Override
    public void offer(Command<?> task) {
        DedupCommand future = new DedupCommand((Command<?>) task);
        DedupCommand current = jobs.putIfAbsent(task.getClass(), future);
        if(current != null) {
            current.replace((Command<?>) task);
        } else {
            executorJob(future);
        }
    }

    protected void executorJob(DedupCommand command) {
        executors.executeCommand(command);
    }

    public class DedupCommand extends AbstractCommand<Void> {

        private Command<?> innerCommand;

        private volatile boolean isRunning = false;

        public DedupCommand(Command<?> innerCommand) {
            this.innerCommand = innerCommand;
        }

        public void replace(Command<?> newCommand) {
            synchronized (this) {
                Command<?> old = this.innerCommand;
                if(!isRunning) {
                    this.innerCommand = newCommand;
                    old.future().cancel(true);
                }
            }
        }

        @Override
        protected void doExecute() throws Exception {
            synchronized (this) {
                isRunning = true;
                jobs.remove(innerCommand.getClass());
            }
            if(innerCommand != null) {
                innerCommand.execute().addListener(new CommandFutureListener() {
                    @Override
                    public void operationComplete(CommandFuture commandFuture) throws Exception {
                        if(commandFuture.isSuccess()) {
                            future().setSuccess();
                        } else {
                            future().setFailure(new XpipeRuntimeException("inner failure", commandFuture.cause()));
                        }
                    }
                });
            }

        }

        @Override
        protected void doReset() {
            isRunning = false;
        }

        @Override
        public String getName() {
            return "WrappedCommand(" + innerCommand.getName() + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DedupCommand that = (DedupCommand) o;
            return innerCommand.getClass().equals(that.innerCommand.getClass());
        }

        @Override
        public int hashCode() {
            return Objects.hash(innerCommand.getClass());
        }

        public synchronized Command<?> getInnerCommand() {
            return innerCommand;
        }
    }

}
