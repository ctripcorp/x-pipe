package com.ctrip.xpipe.redis.core.redis.operation.stream;

import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class StreamCommandParserTest {

    @Test
    public void doRead() {
        List<Object> command = new ArrayList<>();
        RedisOpParser redisOpParser = Mockito.mock(RedisOpParser.class);

        StreamCommandLister streamCommandLister = new StreamCommandLister() {
            @Override
            public void onCommand(Object[] payload, ByteBuf commandBuf) throws IOException {
                command.add(new Object[]{payload, commandBuf});
            }
        };
        StreamCommandParser streamCommandParser = new StreamCommandParser(redisOpParser, streamCommandLister);

        ByteBuf wholeBuf = getArraysRepl(Lists.newArrayList(
                "set",
                "key1",
                getString(20_000_000)
        ));

        long start = System.nanoTime();
        // wholeBuf拆分，要考虑不能整除的情况，最终要处理所有数据，不能缺失
        int piece = wholeBuf.readableBytes() / 512;
        try {
            for (int i = 0; i < piece; i++) {
                ByteBuf part = wholeBuf.readSlice(512);
                streamCommandParser.doRead(part);
            }
            ByteBuf lastPart = wholeBuf.readSlice(wholeBuf.readableBytes());
            streamCommandParser.doRead(lastPart);
        } catch (IOException e) {
            fail();
        }
        long end = System.nanoTime();
        System.out.println("cost: " + (end - start) / 1_000_000 + " ms");
        System.out.println(Arrays.toString(command.toArray()));

    }

    private ByteBuf getArraysRepl(List<String> expected) {
        // *<number-of-elements>\r\n<element-1>...<element-n>
        ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.buffer();
        buffer.writeByte(RedisClientProtocol.ASTERISK_BYTE);
        buffer.writeBytes((String.valueOf(expected.size())).getBytes());
        buffer.writeBytes("\r\n".getBytes());
        for (String s : expected) {
            buffer.writeByte(RedisClientProtocol.DOLLAR_BYTE);
            buffer.writeBytes((String.valueOf(s.length())).getBytes());
            buffer.writeBytes("\r\n".getBytes());
            buffer.writeBytes(s.getBytes());
            buffer.writeBytes("\r\n".getBytes());
        }
        return buffer;
    }

    private String getString(int len) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < len; i++) {
            sb.append('a');
        }
        return sb.toString();
    }
}