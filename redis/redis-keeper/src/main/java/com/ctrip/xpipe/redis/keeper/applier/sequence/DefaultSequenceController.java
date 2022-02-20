package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.keeper.applier.command.ApplierRedisCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.SequenceCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.StubbornCommand;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @author Slight
 * <p>
 * Jan 29, 2022 4:19 PM
 */
public class DefaultSequenceController implements ApplierSequenceController {

    private final Map<RedisKey, SequenceCommand<?>> runningCommands = new HashMap<>();

    Executor stateThread = Executors.newSingleThreadExecutor();
    Executor workerThreads = Executors.newFixedThreadPool(8);

    @Override
    public void submit(ApplierRedisCommand<?> command) {

        if (command.type() == ApplierRedisCommand.ApplierRedisCommandType.SINGLE_KEY) {
            stateThread.execute(()->{
                submitSingleKeyCommand(command);
            });
        }

        //List<RedisKey> keys = command.keys();
        //List<SequenceCommand<?>> pasts = keys.stream().map(runningCommands::get).collect(Collectors.toList());
        //SequenceCommand<?> sequenceCommand = new SequenceCommand<>(pasts, new StubbornCommand<>(command), stateThread, workerThreads);

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

    private void submitSingleKeyCommand(ApplierRedisCommand<?> command) {
        RedisKey key = command.key();
        SequenceCommand<?> current = runningCommands.get(key);
        if (current == null || current.future().isSuccess()) {
            current = new SequenceCommand<>(new StubbornCommand<>(command), stateThread, workerThreads);
        } else if (!current.future().isDone()) {
            current = new SequenceCommand<>(current, new StubbornCommand<>(command), stateThread, workerThreads);
        } else {
            /* current is fail */
            throw new XpipeRuntimeException("UNLIKELY - command keeps retrying til success, unlikely to fail.");
        }

        runningCommands.put(key, current);
        forgetWhenSuccess(current, key);
        current.execute();
    }

    private void forgetWhenSuccess(SequenceCommand<?> sequenceCommand, RedisKey key) {
        sequenceCommand.future().addListener((f)->{
            if (f.isSuccess()) {
                if (sequenceCommand == runningCommands.get(key)) {
                    runningCommands.remove(key);
                }
            }
        });
    }
}
