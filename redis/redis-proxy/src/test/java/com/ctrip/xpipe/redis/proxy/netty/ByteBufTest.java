package com.ctrip.xpipe.redis.proxy.netty;

import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * May 22, 2018
 */
public class ByteBufTest {

    @Test
    public void testByteBuf() {
        ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.buffer();
        System.out.println("readable bytes: " + byteBuf.readableBytes());

        byteBuf.writeBytes(new byte[]{'+', 'P', 'R', 'O', 'X', 'Y'});

        System.out.println("reader index: " + byteBuf.readerIndex());
        System.out.println("readable bytes: " + byteBuf.readableBytes());

        byteBuf.readByte();
        byteBuf.readByte();

        System.out.println("reader index: " + byteBuf.readerIndex());
        System.out.println("readable bytes: " + byteBuf.readableBytes());
    }

    @Test
    public void testSimpleStringParser() {
        SimpleStringParser parser = new SimpleStringParser();
        ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.buffer();
        System.out.println("readable bytes: " + byteBuf.readableBytes());

        byteBuf.writeBytes(new byte[]{'+', 'P', 'R', 'O', 'X', 'Y'});
        SimpleStringParser parser1 = (SimpleStringParser) parser.read(byteBuf);

        System.out.println("parser1 == null: " + (parser1 == null));

        byteBuf.writeBytes(" Route raw://10.3.2.1:6379,raw://10.2.1.1:6380 tls://10.1.1.1:6381".getBytes());
        System.out.println("parser1 == null: " + (parser1 == null));
//        System.out.println(parser1.toString());

        byteBuf.writeBytes("\r\n".getBytes());

        parser1 = (SimpleStringParser) parser.read(byteBuf);
        System.out.println("parser1 == null: " + (parser1 == null));
        System.out.println(parser1.getPayload());
    }

    @Test
    public void predictMaxLength() {
        String protocol = "PROXY route raw://192.168.1.1:6379,raw://192.168.1.1:6379,raw://192.168.1.1:6379,raw://192.168.1.1:6379 " +
                "raw://192.168.1.1:6379,raw://192.168.1.1:6379,raw://192.168.1.1:6379 tls://192.168.1.2:6380" +
                "raw://192.168.1.1:6379,raw://192.168.1.1:6379,raw://192.168.1.1:6379 tls://192.168.1.2:6380;FORWARDFOR 192.168.1.3:5050 " +
                "192.168.1.3:5050 192.168.1.3:5050 192.168.1.3:5050 192.168.1.3:5050 192.168.1.3:5050;COMPRESSED TRUE;COMPRESS_METHOD GZIP";
        System.out.println(new SimpleStringParser(protocol).format().readableBytes());
    }
}
