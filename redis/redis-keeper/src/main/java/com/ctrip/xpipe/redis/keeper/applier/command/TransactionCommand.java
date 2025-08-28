package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.api.command.CommandChain;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpTransactionAdapter;

import java.util.LinkedList;
import java.util.List;

public class TransactionCommand extends AbstractCommand<Boolean> implements RedisOpCommand<Boolean> {

    private RedisOpCommand<?> multiCommand;

    private List<RedisOpCommand<?>> transactionCommands;

    private RedisOpCommand<?> execCommand;

    private RedisOpTransactionAdapter redisOp;

    private CommandChain<Object> commandChain;

    private long commandOffset;

    private GtidSet gtidSet;

    public TransactionCommand() {
        this.transactionCommands = new LinkedList<>();
        this.redisOp = new RedisOpTransactionAdapter();
        this.gtidSet = new GtidSet(GtidSet.EMPTY_GTIDSET);
    }

    private CommandChain<Object> buildCommand() {
        SequenceCommandChain sequenceCommandChain = new SequenceCommandChain(false);
        sequenceCommandChain.add(multiCommand);
        for (RedisOpCommand<?> transactionCommand : transactionCommands) {
            sequenceCommandChain.add(transactionCommand);
        }
        sequenceCommandChain.add(execCommand);
        return sequenceCommandChain;
    }

    private void addCommandOffsetAndGtid(long commandOffset, String gtid) {
        this.commandOffset += commandOffset;
        if(gtid != null) {
            gtidSet.add(gtid);
        }
    }

    public void addTransactionStart(RedisOpCommand<?> multi, long commandOffset, String gtid) {
        multiCommand = multi;
        redisOp.addMultiOp(multi.redisOp());
        addCommandOffsetAndGtid(commandOffset, gtid);
    }

    public void addTransactionEnd(RedisOpCommand<?> exec, long commandOffset, String gtid) {
        execCommand = exec;
        redisOp.addExecOp(exec.redisOp());
        addCommandOffsetAndGtid(commandOffset, gtid);
    }

    public void addTransactionCommands(RedisOpCommand<?> redisOpCommand, long commandOffset, String gtid) {
        transactionCommands.add(redisOpCommand);
        redisOp.addTransactionOp(redisOpCommand.redisOp());
        addCommandOffsetAndGtid(commandOffset, gtid);
    }

    public long commandOffset() {
        return commandOffset;
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

    public GtidSet getGtidSet() {
        return this.gtidSet;
    }
}
