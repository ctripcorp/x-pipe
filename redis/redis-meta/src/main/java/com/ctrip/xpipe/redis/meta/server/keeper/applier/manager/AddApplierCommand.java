package com.ctrip.xpipe.redis.meta.server.keeper.applier.manager;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.ApplierTransMeta;
import com.ctrip.xpipe.redis.core.keeper.applier.container.ApplierContainerErrorCode;
import com.ctrip.xpipe.redis.core.keeper.applier.container.ApplierContainerService;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author ayq
 * <p>
 * 2022/4/2 17:03
 */
public class AddApplierCommand extends AbstractApplierCommand<SlaveRole>{

    private XpipeNettyClientKeyedObjectPool clientKeyedObjectPool;

    public AddApplierCommand(ApplierContainerService applierContainerService, ApplierTransMeta applierTransMeta,
                             XpipeNettyClientKeyedObjectPool clientKeyedObjectPool, ScheduledExecutorService scheduled,
                             int timeoutMilli) {
        super(applierContainerService, applierTransMeta, scheduled, timeoutMilli, 1000);
        this.clientKeyedObjectPool = clientKeyedObjectPool;
    }

    public AddApplierCommand(ApplierContainerService applierContainerService, ApplierTransMeta applierTransMeta,
                            ScheduledExecutorService scheduled, int timeoutMilli, int checkIntervalMilli) {
        super(applierContainerService, applierTransMeta, scheduled, timeoutMilli, checkIntervalMilli);
    }

    @Override
    protected void doApplierContainerOperation() {
        applierContainerService.addOrStartApplier(applierTransMeta);
    }

    @Override
    protected Command<SlaveRole> createCheckStateCommand() {
        return new CheckStateCommand(applierTransMeta.getApplierMeta(), scheduled, clientKeyedObjectPool);
    }

    @Override
    protected boolean isSuccess(ErrorMessage<ApplierContainerErrorCode> error) {
        ApplierContainerErrorCode errorCode = error.getErrorType();
        switch(errorCode){
            case APPLIER_ALREADY_EXIST:
                return true;
            case APPLIER_ALREADY_STARTED:
                return true;
            case INTERNAL_EXCEPTION:
                return false;
            case APPLIER_ALREADY_DELETED:
                return false;
            case APPLIER_ALREADY_STOPPED:
                return false;
            case APPLIER_NOT_EXIST:
                return false;
            default:
                throw new IllegalStateException("unknown state:" + errorCode);
        }
    }

    @Override
    protected void doReset() {

    }

    public static class CheckStateCommand extends AbstractCommand<SlaveRole> {

        private ApplierMeta applierMeta;
        private ScheduledExecutorService scheduled;
        private XpipeNettyClientKeyedObjectPool clientKeyedObjectPool;

        public CheckStateCommand(ApplierMeta applierMeta, ScheduledExecutorService scheduled) {
            this.applierMeta = applierMeta;
            this.scheduled = scheduled;
        }

        public CheckStateCommand(ApplierMeta applierMeta, ScheduledExecutorService scheduled, XpipeNettyClientKeyedObjectPool clientKeyedObjectPool) {
            this.applierMeta = applierMeta;
            this.scheduled = scheduled;
            this.clientKeyedObjectPool = clientKeyedObjectPool;
        }

        @Override
        public String getName() {
            return "[applier role check right command]";
        }

        @Override
        protected void doExecute() throws Exception {

            CommandFuture<Role> future;
            if (clientKeyedObjectPool != null) {
                future = new RoleCommand(clientKeyedObjectPool.getKeyPool(
                        new DefaultEndPoint(applierMeta.getIp(), applierMeta.getPort())), scheduled).execute();
            } else {
                // for unit test purpose
                future = new RoleCommand(applierMeta.getIp(), applierMeta.getPort(), scheduled).execute();
            }

            future.addListener(new CommandFutureListener<Role>() {
                @Override
                public void operationComplete(CommandFuture<Role> commandFuture) throws Exception {

                    if (commandFuture.isSuccess()) {
                        SlaveRole applierRole = (SlaveRole) commandFuture.getNow();
                        if (applierRole.getServerRole() == Server.SERVER_ROLE.APPLIER) {
                            getLogger().info("[doExecute][success]{}", applierRole);
                            future().setSuccess(applierRole);
                        } else {
                            future().setFailure(new ApplierStateNotAsExpectedException(applierMeta, applierRole, Server.SERVER_ROLE.APPLIER));
                        }
                    } else {
                        future().setFailure(commandFuture.cause());
                    }
                }
            });
        }

        @Override
        protected void doReset() {


        }
    }
}
