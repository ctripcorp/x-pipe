package com.ctrip.xpipe.redis.meta.server.keeper.applier.manager;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.netty.TcpPortCheckCommand;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.ApplierTransMeta;
import com.ctrip.xpipe.redis.core.keeper.applier.container.ApplierContainerErrorCode;
import com.ctrip.xpipe.redis.core.keeper.applier.container.ApplierContainerService;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author ayq
 * <p>
 * 2022/4/2 21:36
 */
public class DeleteApplierCommand extends AbstractApplierCommand<Void> {

    public DeleteApplierCommand(ApplierContainerService applierContainerService, ApplierTransMeta applierTransMeta, ScheduledExecutorService scheduled,
                                int timeoutMilli) {
        this(applierContainerService, applierTransMeta, scheduled, timeoutMilli, 1000);
    }

    public DeleteApplierCommand(ApplierContainerService applierContainerService, ApplierTransMeta applierTransMeta, ScheduledExecutorService scheduled,
                               int timeoutMilli, int checkIntervalMilli) {
        super(applierContainerService, applierTransMeta, scheduled, timeoutMilli, checkIntervalMilli);
    }

    @Override
    protected void doApplierContainerOperation() {
        applierContainerService.removeApplier(applierTransMeta);
    }

    @Override
    protected Command<Void> createCheckStateCommand() {
        return new DeleteCheckPortCommand(applierTransMeta.getApplierMeta());
    }

    @Override
    protected boolean isSuccess(ErrorMessage<ApplierContainerErrorCode> error) {
        ApplierContainerErrorCode errorCode = error.getErrorType();
        switch(errorCode){
            case APPLIER_ALREADY_EXIST:
                return false;
            case APPLIER_ALREADY_STARTED:
                return false;
            case INTERNAL_EXCEPTION:
                return false;
            case APPLIER_ALREADY_DELETED:
                return true;
            case APPLIER_ALREADY_STOPPED:
                return false;
            case APPLIER_NOT_EXIST:
                return true;
            default:
                throw new IllegalStateException("unknown state:" + errorCode);
        }
    }

    @Override
    protected void doReset() {
    }

    public static class DeleteCheckPortCommand extends AbstractCommand<Void> {

        private ApplierMeta applierMeta;

        public DeleteCheckPortCommand(ApplierMeta applierMeta) {
            this.applierMeta = applierMeta;
        }

        @Override
        public String getName() {
            return "[check applier deleted]" + applierMeta.desc();
        }

        @Override
        protected void doExecute() throws Exception {

            CommandFuture<Boolean> future = new TcpPortCheckCommand(applierMeta.getIp(), applierMeta.getPort()).execute();
            future.addListener(new CommandFutureListener<Boolean>() {
                @Override
                public void operationComplete(CommandFuture<Boolean> commandFuture) throws Exception {
                    if (commandFuture.isSuccess()) {
                        future().setFailure(new DeleteApplierStillAliveException(applierMeta));
                    } else {
                        future().setSuccess();
                    }
                }
            });
        }

        @Override
        protected void doReset() {

        }
    }
}
