package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.api.command.CommandChain;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpTransactionAdapter;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class TransactionCommand extends AbstractCommand<Boolean> implements RedisOpCommand<Boolean> {

    private ExecutorService workExecutors;

    private RedisOpCommand<?> multiCommand;

    private List<RedisOpCommand<?>> transactionCommands;

    private RedisOpCommand<?> execCommand;

    private RedisOpTransactionAdapter redisOp;

    private CommandChain<Object> commandChain;

    public TransactionCommand(ExecutorService workExecutors) {
        this.workExecutors = workExecutors;
        this.transactionCommands = new LinkedList<>();
        this.redisOp = new RedisOpTransactionAdapter();
    }

    private CommandChain<Object> buildCommand() {
        SequenceCommandChain sequenceCommandChain = new SequenceCommandChain(false);
        sequenceCommandChain.add(multiCommand);
        if (!transactionCommands.isEmpty()) {
            for (RedisOpCommand<?> transactionCommand : transactionCommands) {
                sequenceCommandChain.add(transactionCommand);
            }
        }
        sequenceCommandChain.add(execCommand);
        return sequenceCommandChain;
    }

    public void addTransactionStart(RedisOpCommand<?> multi) {
        multiCommand = multi;
        redisOp.addMultiOp(multi.redisOp());
    }

    public void addTransactionEnd(RedisOpCommand<?> exec) {
        execCommand = exec;
        redisOp.addExecOp(exec.redisOp());
    }

    public void addTransactionCommands(RedisOpCommand<?> redisOpDataCommand) {
        transactionCommands.add(redisOpDataCommand);
        redisOp.addTransactionOp(redisOpDataCommand.redisOp());
    }

    @Override
    protected void doExecute() throws Throwable {
        if (commandChain == null) {
            commandChain = buildCommand();
        }
        commandChain.execute().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                if (commandFuture.isSuccess()) {
                    TransactionCommand.this.future().setSuccess(true);
                } else {
                    TransactionCommand.this.future().setFailure(commandFuture.cause());
                }
            }
        });
    }

    @Override
    protected void doReset() {

    }

    @Override
    public RedisOp redisOp() {
        return redisOp;
    }

    @Override
    public RedisOpCommandType type() {
        return RedisOpCommandType.OTHER;
    }
}
