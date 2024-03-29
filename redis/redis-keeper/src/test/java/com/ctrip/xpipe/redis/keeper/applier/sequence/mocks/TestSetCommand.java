package com.ctrip.xpipe.redis.keeper.applier.sequence.mocks;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpSingleKeyParser;
import com.ctrip.xpipe.redis.keeper.applier.command.RedisOpDataCommand;

import java.util.Arrays;

/**
 * @author Slight
 * <p>
 * Feb 20, 2022 7:11 PM
 */
public class TestSetCommand extends TestSleepCommand implements RedisOpDataCommand<String> {

    private RedisOpParserManager parserManager = new DefaultRedisOpParserManager();

    private RedisOpSingleKeyParser parser = new RedisOpSingleKeyParser(RedisOpType.SET, 1, 2);

    private final String[] rawArgs;

    public TestSetCommand(long duration, String... rawArgs) {
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
