package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.keeper.applier.AbstractInstanceComponent;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import com.ctrip.xpipe.redis.keeper.applier.command.RedisOpCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.SequenceCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.StubbornCommand;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * @author Slight
 * <p>
 * Jan 29, 2022 4:19 PM
 */
public class DefaultSequenceController extends AbstractInstanceComponent implements ApplierSequenceController {

    @InstanceDependency
    public ApplierLwmManager lwmManager;

    final Map<RedisKey, SequenceCommand<?>> runningCommands = new HashMap<>();

    ExecutorService stateThread;
    ExecutorService workerThreads;

    @Override
    protected void doInitialize() throws Exception {
        stateThread = Executors.newSingleThreadExecutor();
        workerThreads = Executors.newFixedThreadPool(8);
    }

    @Override
    protected void doDispose() throws Exception {
        stateThread.shutdown();
        workerThreads.shutdown();
    }

    @Override
    public void submit(RedisOpCommand<?> command) {

        if (command.type() == RedisOpCommand.RedisOpCommandType.SINGLE_KEY) {
            stateThread.execute(()->{
                submitSingleKeyCommand(command);
            });
        }

        if (command.type() == RedisOpCommand.RedisOpCommandType.MULTI_KEY) {
            stateThread.execute(()->{
                submitMultiKeyCommand(command);
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

    private void submitSingleKeyCommand(RedisOpCommand<?> command) {
        RedisKey key = command.key();
        SequenceCommand<?> running = runningCommands.get(key);
        SequenceCommand<?> current = running == null ?
                new SequenceCommand<>(new StubbornCommand<>(command), stateThread, workerThreads) :
                new SequenceCommand<>(running, new StubbornCommand<>(command), stateThread, workerThreads);

        runningCommands.put(key, current);
        forgetWhenSuccess(current, key);
        mergeGtidWhenSuccess(current, command.gtid());
        current.execute();
    }

    private void submitMultiKeyCommand(RedisOpCommand<?> command) {
        List<RedisKey> keys = command.keys();
        List<SequenceCommand<?>> running = keys.stream().map(runningCommands::get).filter(Objects::nonNull).collect(Collectors.toList());
        SequenceCommand<?> current = new SequenceCommand<>(running, new StubbornCommand<>(command), stateThread, workerThreads);

        for (RedisKey key : keys) {
            runningCommands.put(key, current);
            forgetWhenSuccess(current, key);
            mergeGtidWhenSuccess(current, command.gtid());
        }

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

    private void mergeGtidWhenSuccess(SequenceCommand<?> sequenceCommand, String gtid) {
        sequenceCommand.future().addListener((f)->{
            if (f.isSuccess()) {
                if (gtid != null) {
                    lwmManager.submit(gtid);
                }
            }
        });
    }
}
