package com.ctrip.xpipe.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.exception.CommandNotExecuteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BooleanSupplier;

/**
 * @author lishanglin
 * date 2021/5/6
 */
public class ConditionalCommand<T> extends AbstractCommand<T> {

    private Command<T> delegate;

    private BooleanSupplier condition;

    private boolean skipAsSuccess;

    private static final Logger logger = LoggerFactory.getLogger(ConditionalCommand.class);

    public ConditionalCommand(Command<T> delegate, BooleanSupplier condition) {
        this(delegate, condition, false);
    }

    public ConditionalCommand(Command<T> delegate, BooleanSupplier condition, boolean skipAsSuccess) {
        this.delegate = delegate;
        this.condition = condition;
        this.skipAsSuccess = skipAsSuccess;
    }

    @Override
    protected void doExecute() throws Throwable {
        if (!condition.getAsBoolean()) {
            logger.info("[doExecute] skip for condition fail");
            if (skipAsSuccess) future().setSuccess();
            else future().setFailure(new CommandNotExecuteException("conditionally skip"));
            return;
        }

        this.delegate.execute().addListener(commandFuture -> {
            if (commandFuture.isSuccess()) future().setSuccess(commandFuture.get());
            else future().setFailure(commandFuture.cause());
        });
    }

    @Override
    protected void doReset() {
        delegate.reset();
    }

    @Override
    public String getName() {
        return "ConditionalCommand-" + delegate.getName();
    }
}
