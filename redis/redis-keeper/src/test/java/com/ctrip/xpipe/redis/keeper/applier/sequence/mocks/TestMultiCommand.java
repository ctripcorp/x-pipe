package com.ctrip.xpipe.redis.keeper.applier.sequence.mocks;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpNoneKeyParser;
import com.ctrip.xpipe.redis.keeper.applier.command.RedisOpCommand;

import java.util.Arrays;

public class TestMultiCommand extends TestSleepCommand implements RedisOpCommand<String> {

    private RedisOpNoneKeyParser parser = new RedisOpNoneKeyParser(RedisOpType.MULTI);

    private final String[] rawArgs;

    public TestMultiCommand(long duration, String... rawArgs) {
        super(duration);
        this.rawArgs = rawArgs;
    }

    @Override
    public RedisOp redisOp() {
        return parser.parse(rawArgs);
    }

    @Override
    public String toString() {
        return "TestApplierRedisCommand{" +
                "rawArgs=" + Arrays.toString(rawArgs) +
                ", duration=" + duration +
                '}';
    }
}