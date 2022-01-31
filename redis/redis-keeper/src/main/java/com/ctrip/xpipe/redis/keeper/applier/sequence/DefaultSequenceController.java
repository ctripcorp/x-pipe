package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.keeper.applier.client.ApplierRedisClient;
import com.ctrip.xpipe.redis.keeper.applier.client.DoNothingRedisClient;
import com.ctrip.xpipe.redis.keeper.applier.command.ApplierRedisCommand;

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

    private final Map<byte[], CommandFuture<?>> runningCommands = new HashMap<>();

    Executor executor = Executors.newSingleThreadExecutor();

    @Override
    public void submit(ApplierRedisCommand<?> command) {

        byte[] key = command.key();
        SequenceCommand sequenceCommand = new SequenceCommand(command, executor);

        CommandFuture<?> current = runningCommands.get(key);

        if (current == null) {
            runningCommands.put(key, sequenceCommand.execute());
            return;
        }

        if (current.isDone()) {
            if (current.isSuccess()) {
                runningCommands.put(key, sequenceCommand.execute());
            } else {
                current.command().reset();
                current = current.command().execute();

            }
        }
    }
}
