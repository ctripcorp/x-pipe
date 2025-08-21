package com.ctrip.xpipe.redis.keeper.applier.sequence.mocks;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpGtidParser;
import com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpNoneKeyParser;
import com.ctrip.xpipe.redis.keeper.applier.command.RedisOpDataCommand;

import java.util.Arrays;

public class TestExecCommand extends TestSleepCommand implements RedisOpDataCommand<String> {

    private RedisOpParserManager parserManager = new DefaultRedisOpParserManager();

    private RedisOpGtidParser parser = new RedisOpGtidParser(parserManager);

    private final String[] rawArgs;

    public TestExecCommand(long duration, String... rawArgs) {
        super(duration);
        this.rawArgs = rawArgs;
        parserManager.registerParser(RedisOpType.EXEC, new RedisOpNoneKeyParser(RedisOpType.EXEC));
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