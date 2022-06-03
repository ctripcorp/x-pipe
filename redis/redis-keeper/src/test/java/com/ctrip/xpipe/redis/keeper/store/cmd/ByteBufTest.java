package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.netty.commands.ByteBufReceiver;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientHandler;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.utils.ClusterShardAwareThreadFactory;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.IllegalReferenceCountException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * @author lishanglin
 * date 2022/5/23
 */
public class ByteBufTest extends AbstractTest {

    @Test
    public void testReadIndex() {
        ByteBuf byteBuf = Unpooled.directBuffer(1024);
        Assert.assertEquals(0, byteBuf.readableBytes());
        Assert.assertEquals(0, byteBuf.readerIndex());

        writeRandomStrToByteBuf(byteBuf, 512);

        Assert.assertEquals(512, byteBuf.readableBytes());
        Assert.assertEquals(0, byteBuf.readerIndex());

        readByteBuf(byteBuf, 512);
        Assert.assertEquals(0, byteBuf.readableBytes());
        Assert.assertEquals(512, byteBuf.readerIndex());

        byteBuf.readerIndex(0);
        Assert.assertEquals(512, byteBuf.readableBytes());
        Assert.assertEquals(0, byteBuf.readerIndex());
    }

    @Test
    public void testCompositeByteBufReadIndex() {
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(4);

        ByteBuf byteBuf = Unpooled.directBuffer(10);
        writeRandomStrToByteBuf(byteBuf, 10);
        compositeByteBuf.addComponent(true, byteBuf.readBytes(5));
        readByteBuf(byteBuf);

        Assert.assertEquals(0, compositeByteBuf.readerIndex());
        Assert.assertEquals(5, compositeByteBuf.readableBytes());
        Assert.assertEquals(5, compositeByteBuf.writerIndex());

        byteBuf = Unpooled.directBuffer(10);
        writeRandomStrToByteBuf(byteBuf, 10);
        compositeByteBuf.addComponent(true, byteBuf);

        Assert.assertEquals(0, compositeByteBuf.readerIndex());
        Assert.assertEquals(15, compositeByteBuf.readableBytes());
        Assert.assertEquals(15, compositeByteBuf.writerIndex());
    }

    @Test
    public void testByteBufSliceAndRelease() {
        ByteBuf byteBuf = Unpooled.directBuffer(1024);
        writeRandomStrToByteBuf(byteBuf, 1024);

        ByteBuf subByteBuf = byteBuf.slice(0, 512);
        Assert.assertEquals(512, subByteBuf.readableBytes());
        Assert.assertEquals(1024, byteBuf.readableBytes());

        Assert.assertTrue(byteBuf.release());
        Assert.assertEquals(512, subByteBuf.readableBytes());

        try {
            subByteBuf.readByte();
            Assert.fail();
        } catch (IllegalReferenceCountException e) {

        }
    }

    @Test
    public void testCompositeByteBufAndRelease() {
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(4);

        ByteBuf byteBuf1 = Unpooled.directBuffer(10);
        ByteBuf byteBuf2 = Unpooled.directBuffer(10);
        writeRandomStrToByteBuf(byteBuf1, 10);
        writeRandomStrToByteBuf(byteBuf2, 10);
        compositeByteBuf.addComponent(true, byteBuf1);
        compositeByteBuf.addComponent(true, byteBuf2);

        Assert.assertTrue(byteBuf1.release());
        Assert.assertTrue(byteBuf2.release());

        try {
            compositeByteBuf.readByte();
            Assert.fail();
        } catch (IllegalReferenceCountException e) {

        }
    }

    @Test
    @Ignore
    public void manualTestNettyByteBufRelease() throws Exception {
        Server server = startServer("+PONG\r\n");

        EventLoopGroup nioEventLoopGroup = new NioEventLoopGroup(1, XpipeThreadFactory.create("manualTestNettyByteBufRelease-"));
        Bootstrap b = new Bootstrap();
        b.group(nioEventLoopGroup).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LoggingHandler(LogLevel.DEBUG));
                        p.addLast(new NettySimpleMessageHandler());
                        p.addLast(new NettyClientHandler());
                    }
                });

        Channel channel = b.connect("127.0.0.1", server.getPort()).sync().channel();
        NettyClient nettyClient = new DefaultNettyClient(channel);
        channel.attr(NettyClientHandler.KEY_CLIENT).set(nettyClient);
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(4);
        nettyClient.sendRequest(Unpooled.wrappedBuffer(new byte[] {'P', 'I', 'N', 'G', '\r', '\n'}), new ByteBufReceiver() {
            @Override
            public RECEIVER_RESULT receive(Channel channel, ByteBuf byteBuf) {
//                compositeByteBuf.addComponent(true, byteBuf); // inner byteBuf has been already released, and will throw IllegalReferenceCountException
                compositeByteBuf.addComponent(true, byteBuf.retainedSlice()); // retain and should be released later
                return RECEIVER_RESULT.SUCCESS;
            }

            @Override
            public void clientClosed(NettyClient nettyClient) {
                logger.info("[manualTestNettyByteBufRelease] {}", nettyClient.channel());
            }

            @Override
            public void clientClosed(NettyClient nettyClient, Throwable th) {
                logger.info("[manualTestNettyByteBufRelease] {}", nettyClient.channel(), th);
            }
        });

        sleep(10000);
        compositeByteBuf.readByte();
    }

    private void writeRandomStrToByteBuf(ByteBuf byteBuf, int len) {
        for (int i = 0; i < len; i++) {
            byteBuf.writeByte('a' + (int)(26 * Math.random()));
        }
    }

    private void readByteBuf(ByteBuf byteBuf) {
        readByteBuf(byteBuf, byteBuf.readableBytes());
    }

    private void readByteBuf(ByteBuf byteBuf, int len) {
        for (int i = 0; i < len; i++) {
            byteBuf.readByte();
        }
    }

}
