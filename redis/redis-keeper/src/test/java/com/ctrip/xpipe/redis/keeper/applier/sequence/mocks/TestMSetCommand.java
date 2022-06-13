package com.ctrip.xpipe.redis.keeper.applier.sequence.mocks;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpMultiKeysEnum;
import com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpMultiKeysParser;
import com.ctrip.xpipe.redis.keeper.applier.command.RedisOpDataCommand;

import java.util.Arrays;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 13:07
 */
public class TestMSetCommand extends TestSleepCommand implements RedisOpDataCommand<String> {

    private RedisOpParserManager parserManager = new DefaultRedisOpParserManager();

    private RedisOpMultiKeysParser parser = new RedisOpMultiKeysParser(RedisOpType.MSET,
            RedisOpMultiKeysEnum.MSET.getKeyStartIndex(), RedisOpMultiKeysEnum.MSET.getKvNum());

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