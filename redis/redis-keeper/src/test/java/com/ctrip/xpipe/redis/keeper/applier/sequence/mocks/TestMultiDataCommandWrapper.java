package com.ctrip.xpipe.redis.keeper.applier.sequence.mocks;

import com.ctrip.xpipe.redis.keeper.applier.command.MultiDataCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.RedisOpDataCommand;

import java.util.List;
import java.util.concurrent.ExecutorService;

public class TestMultiDataCommandWrapper extends MultiDataCommand {

    private RedisOpDataCommand inner;

    public TestMultiDataCommandWrapper(RedisOpDataCommand inner, ExecutorService workThreads) {
        super(null, inner.redisOpAsMulti(), workThreads);
        this.inner = inner;
    }

    @Override
    public List<RedisOpDataCommand<Boolean>> sharding() {
        return inner.sharding();
    }

    @Override
    public String gtid() {
        return inner.gtid();
    }
}
