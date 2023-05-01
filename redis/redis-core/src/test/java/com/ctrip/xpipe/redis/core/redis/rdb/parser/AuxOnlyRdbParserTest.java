package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseListener;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant.REDIS_RDB_AUX_KEY_GTID;
import static com.ctrip.xpipe.redis.core.redis.rdb.parser.RdbDataBytes.rdbHeaderWithWrongKey;

/**
 * @author lishanglin
 * date 2023/6/14
 */
public class AuxOnlyRdbParserTest extends AbstractTest implements RdbParseListener {

    private AuxOnlyRdbParser parser;

    private DefaultRdbParseContext context;

    private AtomicReference<String> gtidSet = new AtomicReference<>();

    @Before
    public void setupDefaultRdbParserTest() {
        context = new DefaultRdbParseContext();
        parser = new AuxOnlyRdbParser(context);
        parser.registerListener(this);
        parser.needFinishNotify(true);
    }

    @Test
    public void testParseStopOnSelectDB() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(rdbHeaderWithWrongKey);
        while (!parser.isFinish() || byteBuf.readableBytes() <= 0) {
            parser.read(byteBuf);
        }

        Assert.assertTrue(parser.isFinish());
        Assert.assertNotNull(gtidSet);
        Assert.assertEquals(RdbParseContext.RdbType.SELECTDB, context.getCurrentType());
    }

    @Override
    public void onRedisOp(RedisOp redisOp) {
        logger.info("[onRedisOp] {}", redisOp);
    }

    @Override
    public void onAux(String key, String value) {
        logger.info("[onAux] {} {}", key, value);
        if (REDIS_RDB_AUX_KEY_GTID.equalsIgnoreCase(key)) {
            gtidSet.set(value);
        }
    }

    @Override
    public void onFinish(RdbParser<?> parser) {
        logger.info("[onFinish] {}", parser);
    }

    @Override
    public void onAuxFinish() {
        logger.info("[onAuxFinish]");
    }
}
