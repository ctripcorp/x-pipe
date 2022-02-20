package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.keeper.applier.command.ApplierRedisCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.SequenceCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.StubbornCommand;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @author Slight
 * <p>
 * Jan 29, 2022 4:19 PM
 */
public class DefaultSequenceController implements ApplierSequenceController {

    private final Map<RedisKey, CommandFuture<?>> runningCommands = new HashMap<>();

    Executor stateThread = Executors.newSingleThreadExecutor();
    Executor workerThreads = Executors.newFixedThreadPool(8);

    @Override
    public void submit(ApplierRedisCommand<?> command) {
        List<RedisKey> keys = command.keys();
        SequenceCommand sequenceCommand = new SequenceCommand(new StubbornCommand(command), stateThread, workerThreads);

        //CommandFuture<?> current = runningCommands.get(key);

        //if (current == null) {
        //    runningCommands.put(key, sequenceCommand.execute());
        //    return;
        //}

        //if (current.isDone()) {
        //    if (current.isSuccess()) {
        //        runningCommands.put(key, sequenceCommand.execute());
        //    } else {
        //        current.command().reset();
        //        current = current.command().execute();
        //    }
        //}
    }
}
