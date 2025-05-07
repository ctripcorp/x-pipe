package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class IndexTestTool {

    private static RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
    private static RedisOpParser opParser;

    static {
        opParser = new GeneralRedisOpParser(redisOpParserManager);
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
    }

    public static ByteBuf readBytebufPre(String filePath, long offset) {
        long length = Math.min(offset - 0, 128);   // 要读取的字节数
        try (FileInputStream fis = new FileInputStream(filePath);
             FileChannel fileChannel = fis.getChannel()) {
            ByteBuffer buffer = ByteBuffer.allocate((int)length);
            fileChannel.position(offset - length); // 移动到指定的偏移量
            int bytesRead = fileChannel.read(buffer);
            if (bytesRead != -1) {
                ByteBuf byteBuf = Unpooled.wrappedBuffer(buffer.array());
                return byteBuf;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static RedisOp readBytebufAfter(String filePath, long offset) {

        RedisOp result = null;

        long length = 256;   // 要读取的字节数
        try (FileInputStream fis = new FileInputStream(filePath);
             FileChannel fileChannel = fis.getChannel()) {

            ByteBuffer buffer = ByteBuffer.allocate((int)length);
            fileChannel.position(offset); // 移动到指定的偏移量
            int bytesRead = fileChannel.read(buffer);
            buffer.flip();
            ByteBuf byteBuf = null;
            if (bytesRead != -1) {
                byteBuf = Unpooled.wrappedBuffer(buffer.array());
            } else {
                return null;
            }

            while (result == null || StringUtil.isEmpty(result.getOpGtid())) {
                result = IndexTestTool.readByteBuf(byteBuf);
            }
            return result;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }



    public static RedisOp readByteBuf(ByteBuf byteBuf) {
        RedisClientProtocol<Object[]> protocolParser = new ArrayParser();
        RedisClientProtocol<Object[]> protocol = protocolParser.read(byteBuf);
        Assert.assertNotNull(protocol);
        Object[] payload = protocol.getPayload();
        RedisOp redisOp = opParser.parse(payload);
        return redisOp;
    }
}
