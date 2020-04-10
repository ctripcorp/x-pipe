package com.ctrip.xpipe.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;

import java.util.concurrent.Executor;

public class LogCommandWrapper<V> implements Command<V> {

    private Command<V> innerCommand;

    private boolean needLog;

    public LogCommandWrapper(Command<V> command, boolean needLog) {
        this.innerCommand = command;
        this.needLog = needLog;
    }

    public CommandFuture<V> future() {
        return innerCommand.future();
    }

    public CommandFuture<V> execute() {
        return innerCommand.execute();
    }

    public CommandFuture<V> execute(Executor executors) {
        return innerCommand.execute(executors);
    }

    public String getName() {
        return innerCommand.getName();
    }

    public void reset() {
        innerCommand.reset();
    }

    public boolean isNeedLog() {
        return needLog;
    }

    public Command<V> getInnerCommand() {
        return innerCommand;
    }

}
