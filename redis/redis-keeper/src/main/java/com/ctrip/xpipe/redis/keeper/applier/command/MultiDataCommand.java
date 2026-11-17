package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class MultiDataCommand extends AbstractCommand<Boolean> implements RedisOpDataCommand<Boolean> {

    private static final Logger logger = LoggerFactory.getLogger(MultiDataCommand.class);

    final AsyncRedisClient client;

    final RedisMultiKeyOp redisMultiKeyOp;

    final ExecutorService workThreads;
    final int dbNumber;

    public MultiDataCommand(AsyncRedisClient client, RedisMultiKeyOp redisMultiKeyOp, int dbNumber, ExecutorService workThreads) {
        this.client = client;
        this.redisMultiKeyOp = redisMultiKeyOp;
        this.workThreads = workThreads;
        this.dbNumber = dbNumber;
    }

    @SuppressWarnings("rawtypes")
    protected List<RedisOpDataCommand> sharding() {
        List<Object> keys = keys().stream().map(RedisKey::get).collect(Collectors.toList());
        Map<Object, List<Object>> resourceKeys = client.selectMulti(keys);
        if (redisMultiKeyOp.getOpType().isSameGroupCommand()) {
            if (resourceKeys.size() != 1) {
                logger.error("[sharding] same group command {} spans {} target groups, keys: {}",
                        redisMultiKeyOp.getOpType(), resourceKeys.size(), keys);
                EventMonitor.DEFAULT.logError("APPLIER.SHARDING.MultiKeyWrongGroup", redisMultiKeyOp.getOpType().name(),
                        Map.of("targetGroupCount", String.valueOf(resourceKeys.size()),
                                "keyCount", String.valueOf(keys.size())));
                throw new IllegalStateException("same group command spans multiple target groups: " + redisMultiKeyOp.getOpType());
            }
            return Collections.singletonList(new DefaultDataCommand(client, resourceKeys.keySet().iterator().next(), redisMultiKeyOp, dbNumber));
        }
        return resourceKeys.entrySet().stream().map(e ->
                new DefaultDataCommand(client,
                        /* resource */ e.getKey(),
                        /* subOp */ redisOpAsMulti().subOp(e.getValue().stream().map(keys::indexOf).collect(Collectors.toSet())),
                        dbNumber)).collect(Collectors.toList());
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void doExecute() throws Throwable {
        RedisOpDataCommand[] dataCommands = sharding().toArray(new RedisOpDataCommand[0]);
        ParallelCommandChain parallelCommandChain = new ParallelCommandChain(workThreads, false);
        for (RedisOpDataCommand dataCommand : dataCommands) {
            parallelCommandChain.add(dataCommand);
        }
        parallelCommandChain.execute().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                if (commandFuture.isSuccess()) {
                    MultiDataCommand.this.future().setSuccess(true);
                } else {
                    MultiDataCommand.this.future().setFailure(commandFuture.cause());
                }
            }
        });
    }

    @Override
    protected void doReset() {

    }

    @Override
    public RedisOp redisOp() {
        return redisMultiKeyOp;
    }

    public int getDbNumber(){
        return this.dbNumber;
    }
}
