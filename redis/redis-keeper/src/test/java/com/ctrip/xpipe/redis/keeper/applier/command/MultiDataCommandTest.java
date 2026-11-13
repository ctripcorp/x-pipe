package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpMultiKeysParser;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MultiDataCommandTest {

    @Test
    public void testSameGroupCommandKeepsCompleteArguments() {
        Assert.assertTrue(RedisOpType.PFCOUNT.isSameGroupCommand());
        Assert.assertTrue(RedisOpType.PFMERGE.isSameGroupCommand());
        Assert.assertTrue(RedisOpType.LMPOP.isSameGroupCommand());
        Assert.assertTrue(RedisOpType.BLMPOP.isSameGroupCommand());
        Assert.assertTrue(RedisOpType.ZMPOP.isSameGroupCommand());
        Assert.assertTrue(RedisOpType.BZMPOP.isSameGroupCommand());

        AsyncRedisClient client = Mockito.mock(AsyncRedisClient.class);
        Object resource = new Object();
        Mockito.when(client.selectMulti(Mockito.anyList())).thenAnswer(invocation -> {
            List<Object> keys = invocation.getArgument(0);
            Map<Object, List<Object>> resourceKeys = new LinkedHashMap<>();
            resourceKeys.put(resource, new ArrayList<>(keys));
            return resourceKeys;
        });
        byte[][] rawArgs = bytes("LMPOP", "2", "list-1", "list-2", "LEFT", "COUNT", "10");
        RedisMultiKeyOp redisOp = (RedisMultiKeyOp) new RedisOpMultiKeysParser(RedisOpType.LMPOP, 2, 1).parse(rawArgs);

        List<RedisOpDataCommand> commands = new ExposedMultiDataCommand(client, redisOp, 15).shardForTest();

        Assert.assertEquals(1, commands.size());
        DefaultDataCommand command = (DefaultDataCommand) commands.get(0);
        Assert.assertEquals(15, command.getDbNumber());
        Assert.assertArrayEquals(rawArgs, command.redisOp().buildRawOpArgs());
    }

    @Test
    public void testSameGroupCommandRejectsMultipleGroups() {
        AsyncRedisClient client = Mockito.mock(AsyncRedisClient.class);
        Mockito.when(client.selectMulti(Mockito.anyList())).thenAnswer(invocation -> {
            List<Object> keys = invocation.getArgument(0);
            Map<Object, List<Object>> resourceKeys = new LinkedHashMap<>();
            resourceKeys.put(new Object(), Arrays.asList(keys.get(0)));
            resourceKeys.put(new Object(), Arrays.asList(keys.get(1)));
            return resourceKeys;
        });
        RedisMultiKeyOp redisOp = (RedisMultiKeyOp) new RedisOpMultiKeysParser(RedisOpType.PFMERGE, 1, 1)
                .parse(bytes("PFMERGE", "destination", "source"));

        IllegalStateException exception = Assert.assertThrows(IllegalStateException.class,
                () -> new ExposedMultiDataCommand(client, redisOp, 0).shardForTest());

        Assert.assertEquals("same group command spans multiple target groups: PFMERGE", exception.getMessage());
    }

    private byte[][] bytes(String... values) {
        byte[][] result = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            result[i] = values[i].getBytes(StandardCharsets.US_ASCII);
        }
        return result;
    }

    private static class ExposedMultiDataCommand extends MultiDataCommand {

        ExposedMultiDataCommand(AsyncRedisClient client, RedisMultiKeyOp redisOp, int dbNumber) {
            super(client, redisOp, dbNumber, MoreExecutors.newDirectExecutorService());
        }

        List<RedisOpDataCommand> shardForTest() {
            return sharding();
        }
    }
}
