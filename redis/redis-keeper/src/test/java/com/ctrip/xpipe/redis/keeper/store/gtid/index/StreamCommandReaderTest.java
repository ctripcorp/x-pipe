package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class StreamCommandReaderTest {

    RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();

    RedisOpParser opParser;

    private StreamCommandReader commandReader;

    String filePath = "src/test/resources/GtidTest/appendonly.aof";

    @Before
    public void beforeCommandReaderTest() {
        opParser = new GeneralRedisOpParser(redisOpParserManager);
        commandReader = new StreamCommandReader(0, opParser);
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
    }

    ByteBuf readBytebuf(long offset) {
        int length = 128;   // 要读取的字节数
        try (FileInputStream fis = new FileInputStream(filePath);
             FileChannel fileChannel = fis.getChannel()) {
            ByteBuffer buffer = ByteBuffer.allocate(length);
            fileChannel.position(offset); // 移动到指定的偏移量
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

    @Test
    public void test() throws IOException {
        File f = new File(filePath);
        ControllableFile controllableFile = new DefaultControllableFile(f);
        controllableFile.getFileChannel().position(0);

        StreamCommandListener listener = new StreamCommandListener() {
            @Override
            public void onCommand(String gtid, long offset) {
                RedisClientProtocol<Object[]> protocolParser = new ArrayParser();
                RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
                ByteBuf byteBuf = readBytebuf(offset);
                RedisClientProtocol<Object[]> protocol = protocolParser.read(byteBuf);
                Assert.assertNotNull(protocol);
                Object[] payload = protocol.getPayload();
                RedisOp redisOp = opParser.parse(payload);
                Assert.assertEquals(redisOp.getOpGtid(), gtid);
            }
        };
        commandReader.addListener(listener);


        while(controllableFile.getFileChannel().position() < controllableFile.getFileChannel().size()) {
            int size = (int)Math.min(1024, controllableFile.getFileChannel().size() - controllableFile.getFileChannel().position());
            ByteBuffer buffer = ByteBuffer.allocate(size);
            controllableFile.getFileChannel().read(buffer);
            buffer.flip();
            ByteBuf byteBuf = Unpooled.wrappedBuffer(buffer.array());
            commandReader.doRead(byteBuf);
        }
    }
}
