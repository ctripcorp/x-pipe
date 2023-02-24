package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.client.redis.DoNothingRedisClient;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.keeper.applier.sequence.mocks.TestExecCommand;
import com.ctrip.xpipe.redis.keeper.applier.sequence.mocks.TestMultiCommand;
import com.ctrip.xpipe.redis.keeper.applier.sequence.mocks.TestSetCommand;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class TransactionCommandTest extends AbstractTest {


    @Test
    public void testCreate() throws ExecutionException, InterruptedException {

        TransactionCommand transactionCommand = new TransactionCommand(new DoNothingRedisClient(), executors);
        TestMultiCommand multiCommand = spy(new TestMultiCommand(100, "MULTI"));
        transactionCommand.addTransactionStart(multiCommand);
        TestSetCommand dataCommand = spy(new TestSetCommand(100, "SET", "K", "V"));
        transactionCommand.addTransactionCommands(dataCommand);
        TestExecCommand execCommand = spy(new TestExecCommand(100, "GTID", "A:2", "0", "EXEC"));
        transactionCommand.addTransactionEnd(execCommand);

        Assert.assertEquals(transactionCommand.redisOp().getOpType(), RedisOpType.MULTI);
        Assert.assertEquals(transactionCommand.gtid(), "A:2");
        Assert.assertEquals(transactionCommand.type(), RedisOpCommandType.OTHER);
        Assert.assertEquals(transactionCommand.redisOp().estimatedSize(),
                multiCommand.redisOp().estimatedSize() + dataCommand.redisOp().estimatedSize() + execCommand.redisOp().estimatedSize());

        transactionCommand.execute().get();

        verify(multiCommand).execute(any());
        verify(dataCommand).execute(any());
        verify(execCommand).execute(any());

        Assert.assertTrue(dataCommand.startTime > multiCommand.endTime);
        Assert.assertTrue(execCommand.startTime > dataCommand.endTime);
    }
}