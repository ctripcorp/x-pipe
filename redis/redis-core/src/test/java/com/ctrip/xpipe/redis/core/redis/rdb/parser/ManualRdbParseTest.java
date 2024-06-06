package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseListener;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author lishanglin
 * date 2022/6/14
 */
public class ManualRdbParseTest extends AbstractTest implements RdbParseListener {

    private String filePath = "/Users/ccsa/prog/demo/redis-test/redis6379/data/dump6379.rdb";

    private DefaultRdbParser rdbParser;

    @Before
    public void setupManualRdbParseTest() {
        rdbParser = new DefaultRdbParser();
        rdbParser.registerListener(this);
        rdbParser.needFinishNotify(true);
    }

    @Test
    public void parseRdbFileTest() throws Exception {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            throw new IllegalArgumentException("file not exist or not file");
        }

        ControllableFile controllableFile = new DefaultControllableFile(file);
        ByteBuf byteBuf = null;
        controllableFile.getFileChannel().position(0);

        while (controllableFile.size() > controllableFile.getFileChannel().position()) {

            ByteBuffer cmdBuffer = ByteBuffer.allocateDirect(512);
            byteBuf = Unpooled.wrappedBuffer(cmdBuffer);
            controllableFile.getFileChannel().read(cmdBuffer);

            if (cmdBuffer.position() < cmdBuffer.capacity()) {
                byteBuf.capacity(cmdBuffer.position());
            }
            rdbParser.read(byteBuf);
        }
    }

    @Override
    public void onRedisOp(RedisOp redisOp) {
        logger.info("[onRedisOp] {}", redisOp);
    }

    @Override
    public void onAux(String key, String value) {
        logger.info("[onAux] {} {}", key, value);
    }

    @Override
    public void onFinish(RdbParser<?> parser) {
        logger.info("[onFinish] {}", parser);
    }

    @Override
    public void onAuxFinish(Map<String, String> auxMap) {
        logger.info("[onAuxFinish] {}", auxMap);
    }

}
