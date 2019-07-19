package com.ctrip.xpipe.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;

public class CausalChain extends AbstractCommandChain {

    private boolean failContinue = false;

    @Override
    protected void doExecute() throws Exception {
        super.doExecute();
        executeChain();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected void executeChain() {

        if(future().isCancelled()) {
            return;
        }

        CommandFuture<?> currentFuture = executeNext();
        if(currentFuture == null) {
            future().setSuccess(getResult());
            return;
        }
        Command next = getCommand(executeCount());
        if (next instanceof Causal) {
            CausalCommand nextCommand = (CausalCommand) getCommand(executeCount());
            if (nextCommand != null) {
                nextCommand.getCausation(currentFuture);
            }
        }
        currentFuture.addListener(new CommandFutureListener() {

            @Override
            public void operationComplete(CommandFuture commandFuture) throws Exception {

                if(commandFuture.isSuccess()) {
                    executeChain();
                } else {
                    failExecuteNext(commandFuture);
                }
            }
        });
    }

    private void failExecuteNext(CommandFuture<?> commandFuture) {

        logger.error("[failExecuteNext]" + commandFuture.command(), commandFuture.cause());

        if(failContinue){
            executeChain();
            return;
        }

        future().setFailure(new CommandChainException("causal chain, fail stop", getResult()));
    }


}
