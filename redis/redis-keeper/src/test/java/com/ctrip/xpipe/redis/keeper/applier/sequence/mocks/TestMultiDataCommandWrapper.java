package com.ctrip.xpipe.redis.keeper.applier.sequence.mocks;

import com.ctrip.xpipe.redis.keeper.applier.command.MultiDataCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.RedisOpDataCommand;

import java.util.List;
import java.util.concurrent.ExecutorService;

public class TestMultiDataCommandWrapper extends MultiDataCommand {

    private RedisOpDataCommand inner;

    private List<RedisOpDataCommand> subCommands;

    public TestMultiDataCommandWrapper(RedisOpDataCommand inner, ExecutorService workThreads,  List<RedisOpDataCommand> redisOpDataCommands) {
        super(null, inner.redisOpAsMulti(), workThreads);
        this.inner = inner;
        this.subCommands = redisOpDataCommands;
    }

    @Override
    public List<RedisOpDataCommand> sharding() {
        return subCommands;
    }

    @Override
    public String gtid() {
        return inner.gtid();
    }
}
