package com.ctrip.xpipe.redis.meta.server.keeper.applier.manager;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.retry.RetryPolicy;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.CommandRetryWrapper;
import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.redis.core.entity.ApplierTransMeta;
import com.ctrip.xpipe.redis.core.keeper.applier.container.ApplierContainerErrorCode;
import com.ctrip.xpipe.redis.core.keeper.applier.container.ApplierContainerException;
import com.ctrip.xpipe.redis.core.keeper.applier.container.ApplierContainerService;
import com.ctrip.xpipe.retry.RetryDelay;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author ayq
 * <p>
 * 2022/4/2 16:50
 */
public abstract class AbstractApplierCommand<V> extends AbstractCommand<V> {

    protected final ApplierContainerService applierContainerService;
    protected final ApplierTransMeta applierTransMeta;
    protected ScheduledExecutorService scheduled;
    protected int  timeoutMilli;
    protected int  checkIntervalMilli = 1000;

    public AbstractApplierCommand(ApplierContainerService applierContainerService, ApplierTransMeta applierTransMeta, ScheduledExecutorService scheduled, int timeoutMilli, int checkIntervalMilli) {
        this.applierContainerService = applierContainerService;
        this.applierTransMeta = applierTransMeta;
        this.scheduled = scheduled;
        this.timeoutMilli = timeoutMilli;
        this.checkIntervalMilli = checkIntervalMilli;
    }


    @Override
    protected void doExecute() throws Exception {

        getLogger().info("[doExecute]{}", this);
        doOperation();
    }

    @SuppressWarnings("unchecked")
    private void doOperation() {
        try{
            doApplierContainerOperation();
            checkUntilStateOk();
        }catch(ApplierContainerException e){
            ErrorMessage<ApplierContainerErrorCode> error = (ErrorMessage<ApplierContainerErrorCode>) e.getErrorMessage();
            if(error != null && isSuccess(error)){
                checkUntilStateOk();
            }else{
                future().setFailure(e);
            }
        }catch(Exception e){
            future().setFailure(e);
        }
    }

    protected void checkUntilStateOk(){

        CommandRetryWrapper.buildTimeoutRetry(timeoutMilli,
                createRetryPolicy(),
                createCheckStateCommand(), scheduled).execute().addListener(new CommandFutureListener<V>() {

            @Override
            public void operationComplete(CommandFuture<V> commandFuture) throws Exception {

                if(commandFuture.isSuccess()){
                    getLogger().info("[checkUntilStateOk][ok]{}", AbstractApplierCommand.this);
                    future().setSuccess(commandFuture.get());
                }else{
                    getLogger().info("[checkUntilStateOk][fail]{}, {}", AbstractApplierCommand.this, commandFuture.cause());
                    future().setFailure(commandFuture.cause());
                }
            }
        });
    }

    protected RetryPolicy createRetryPolicy() {
        return new RetryDelay(checkIntervalMilli);
    }

    protected abstract Command<V> createCheckStateCommand();

    protected abstract boolean isSuccess(ErrorMessage<ApplierContainerErrorCode> error);

    protected abstract void doApplierContainerOperation();


    @Override
    public String getName() {
        return String.format("[%s(%s)]", getClass().getSimpleName(), applierTransMeta);
    }
}
