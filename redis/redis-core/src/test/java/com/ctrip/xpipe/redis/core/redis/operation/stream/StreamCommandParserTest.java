package com.ctrip.xpipe.redis.core.redis.operation.stream;

import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class StreamCommandParserTest {

    @Test
    public void doRead_normalTest() {
        List<Object> command = new ArrayList<>();

        StreamCommandLister streamCommandLister = new StreamCommandLister() {
            @Override
            public void onCommand(Object[] payload, ByteBuf commandBuf) throws IOException {
                command.add(new Object[]{payload, commandBuf});
            }
        };
        StreamCommandParser streamCommandParser = new StreamCommandParser(streamCommandLister);

        ByteBuf byteBuf = getArraysRepl(Lists.newArrayList(
                "set",
                "key1",
                "value1"
        ));
        int readableBytes = byteBuf.readableBytes();

        try {
            streamCommandParser.doRead(byteBuf);
        } catch (IOException e) {
            fail();
        }
        assertEquals(1, command.size());
        Object[] first = (Object[]) command.get(0);
        assertEquals(3, ((Object[]) first[0]).length);
        assertEquals("set", ((Object[]) first[0])[0].toString());
        assertEquals("key1", ((Object[]) first[0])[1].toString());
        assertEquals("value1", ((Object[]) first[0])[2].toString());
        ByteBuf cmdBuf = (ByteBuf) first[1];
        assertEquals(readableBytes, cmdBuf.readableBytes());
    }

    @Test
    public void doRead_abnormalTest() {
        List<Object> command = new ArrayList<>();

        StreamCommandLister streamCommandLister = new StreamCommandLister() {
            @Override
            public void onCommand(Object[] payload, ByteBuf commandBuf) throws IOException {
                command.add(new Object[]{payload, commandBuf});
            }
        };
        StreamCommandParser streamCommandParser = new StreamCommandParser(streamCommandLister);

        ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.buffer();
        byteBuf.writeBytes("xxxxxx".getBytes());
        byteBuf.writeBytes(getArraysRepl(Lists.newArrayList(
                "set",
                "key1",
                "value1"
        )));

        int readableBytes = byteBuf.readableBytes();

        try {
            streamCommandParser.doRead(byteBuf);
        } catch (IOException e) {
            fail();
        }
        assertEquals(1, command.size());
        Object[] first = (Object[]) command.get(0);
        assertEquals(3, ((Object[]) first[0]).length);
        assertEquals("set", ((Object[]) first[0])[0].toString());
        assertEquals("key1", ((Object[]) first[0])[1].toString());
        assertEquals("value1", ((Object[]) first[0])[2].toString());
        ByteBuf cmdBuf = (ByteBuf) first[1];
        assertEquals(readableBytes - 6, cmdBuf.readableBytes());
    }

    @Test
    public void doRead_ReConnectTest() {
        // 模拟发送部分包的时候，连接断开，然后重连后从头开始发送，结果正确
        List<Object> command = new ArrayList<>();
        StreamCommandLister streamCommandLister = new StreamCommandLister() {
            @Override
            public void onCommand(Object[] payload, ByteBuf commandBuf) throws IOException {
                command.add(new Object[]{payload, commandBuf});
            }
        };
        StreamCommandParser streamCommandParser = new StreamCommandParser(streamCommandLister);
        ByteBuf wholeBuf = getArraysRepl(Lists.newArrayList(
                "set",
                "key1",
                "value1"
        ));
        // 发送部分包
        ByteBuf part = wholeBuf.readSlice(10);
        try {
            streamCommandParser.doRead(part);
        } catch (IOException e) {
            fail();
        }
        assertEquals(0, command.size());
        // 重连
        streamCommandParser.reset();
        // 重新发送所有
        wholeBuf.readerIndex(0);
        try {
            streamCommandParser.doRead(wholeBuf);
        } catch (IOException e) {
            fail();
        }
        assertEquals(1, command.size());
        Object[] first = (Object[]) command.get(0);
        assertEquals(3, ((Object[]) first[0]).length);
        assertEquals("set", ((Object[]) first[0])[0].toString());
        assertEquals("key1", ((Object[]) first[0])[1].toString());
        assertEquals("value1", ((Object[]) first[0])[2].toString());
    }

    @Test
    public void doRead_bigKeysTest() {
        List<Object> command = new ArrayList<>();

        StreamCommandLister streamCommandLister = new StreamCommandLister() {
            @Override
            public void onCommand(Object[] payload, ByteBuf commandBuf) throws IOException {
                command.add(new Object[]{payload, commandBuf});
            }
        };
        StreamCommandParser streamCommandParser = new StreamCommandParser(streamCommandLister);

        ByteBuf wholeBuf = getArraysRepl(Lists.newArrayList(
                "set",
                "key1",
                getString(20_000_000)
        ));

        for (int r = 0; r < 10; r++) {
            long start = System.nanoTime();
            // wholeBuf拆分，要考虑不能整除的情况，最终要处理所有数据，不能缺失
            int size = 65536;
            int piece = wholeBuf.readableBytes() / size;
            try {
                for (int i = 0; i < piece; i++) {
                    ByteBuf part = wholeBuf.readSlice(size);
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

            wholeBuf.readerIndex(0);
            streamCommandParser.reset();
            command.clear();
        }

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