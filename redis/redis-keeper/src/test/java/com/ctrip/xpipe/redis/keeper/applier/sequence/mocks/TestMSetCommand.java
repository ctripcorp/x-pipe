package com.ctrip.xpipe.redis.keeper.applier.sequence.mocks;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpMsetParser;
import com.ctrip.xpipe.redis.keeper.applier.command.RedisOpDataCommand;
import org.assertj.core.util.Lists;

import java.util.Arrays;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 13:07
 */
public class TestMSetCommand extends TestSleepCommand implements RedisOpDataCommand<String> {

    private RedisOpParserManager parserManager = new DefaultRedisOpParserManager();

    private RedisOpMsetParser parser = new RedisOpMsetParser(parserManager);

    private final String[] rawArgs;

    public TestMSetCommand(long duration, String... rawArgs) {
        super(duration);
        this.rawArgs = rawArgs;
    }

    @Override
    public RedisOp redisOp() {
        return parser.parse(Arrays.stream(rawArgs).map(String::getBytes).toArray());
    }

    @Override
    public String toString() {
        return "TestMSetCommand{" +
                "parserManager=" + parserManager +
                ", parser=" + parser +
                ", rawArgs=" + Arrays.toString(rawArgs) +
                '}';
    }
}