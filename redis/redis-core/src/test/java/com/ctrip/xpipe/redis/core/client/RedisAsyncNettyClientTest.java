package com.ctrip.xpipe.redis.core.client;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.ByteBufReceiver;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientHandler;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.simpleserver.AbstractIoAction;
import com.ctrip.xpipe.simpleserver.IoAction;
import com.ctrip.xpipe.simpleserver.IoActionFactory;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class RedisAsyncNettyClientTest extends AsyncNettyClientTest {

    private static final String prefix = "RedisAsyncNettyClientTest";

    @Before
    public void beforeRedisAsyncNettyClientTestTest() throws Exception{
        server = startEchoServer(randomPort(), prefix);
    }

    @Test
    public void testStickyBag() throws Exception {
        server = startEchoServer(randomPort(), "+OK\r\n+OK1\r\n+OK2");
        RedisAsyncNettyClient client = new RedisAsyncNettyClient(b.connect("localhost", server.getPort()),
                new DefaultEndPoint("localhost", server.getPort()), "xpipe", () -> true);
        client.channel().attr(NettyClientHandler.KEY_CLIENT).set(client);

        StringBuffer sb = new StringBuffer();

        StringBuilder expected = new StringBuilder();

        String message = "+1" + "\r\n";
        final int[] minReadAbleBytes = {Integer.MAX_VALUE};
        client.sendRequest(Unpooled.copiedBuffer(message.getBytes()), new ByteBufReceiver() {

            private RedisClientProtocol<String> parser = new SimpleStringParser();
            @Override
            public RECEIVER_RESULT receive(Channel channel, ByteBuf byteBuf) {
                RedisClientProtocol<String> clientProtocol = parser.read(byteBuf);
                if(clientProtocol != null) {
                    sb.append(clientProtocol.getPayload());
                    if (byteBuf.readableBytes() < minReadAbleBytes[0]) {
                        minReadAbleBytes[0] = byteBuf.readableBytes();
                    }
                    return RECEIVER_RESULT.SUCCESS;
                }
                return RECEIVER_RESULT.CONTINUE;
            }

            @Override
            public void clientClosed(NettyClient nettyClient) {

            }

            @Override
            public void clientClosed(NettyClient nettyClient, Throwable th) {

            }
        });
        expected.append("OK1");
        waitConditionUntilTimeOut(()->client.channel().isActive(), 1000);
        sleep(1000);
        String str = sb.toString();
        Assert.assertTrue(minReadAbleBytes[0] > 0);
        Assert.assertTrue(client.getDoAfterConnectedSuccess());
        Assert.assertEquals(str, expected.toString());
    }

    @Test
    public void testUnpacking() throws Exception {
        server = startUnpackingServer(randomPort(), "+O");
        RedisAsyncNettyClient client = new RedisAsyncNettyClient(b.connect("localhost", server.getPort()),
                new DefaultEndPoint("localhost", server.getPort()), "xpipe", () -> true);
        client.channel().attr(NettyClientHandler.KEY_CLIENT).set(client);

        StringBuffer sb = new StringBuffer();

        StringBuilder expected = new StringBuilder();
        String message = "K\r\nOK1\r\n";
        client.sendRequest(Unpooled.copiedBuffer(message.getBytes()), new ByteBufReceiver() {

            private RedisClientProtocol<String> parser = new SimpleStringParser();
            @Override
            public RECEIVER_RESULT receive(Channel channel, ByteBuf byteBuf) {
                RedisClientProtocol<String> clientProtocol = parser.read(byteBuf);
                if(clientProtocol != null) {
                    sb.append(clientProtocol.getPayload());
                    return RECEIVER_RESULT.SUCCESS;
                }
                return RECEIVER_RESULT.CONTINUE;
            }

            @Override
            public void clientClosed(NettyClient nettyClient) {

            }

            @Override
            public void clientClosed(NettyClient nettyClient, Throwable th) {

            }
        });
        expected.append("OK1");
        waitConditionUntilTimeOut(()->client.channel().isActive(), 1000);
        sleep(1000);
        String str = sb.toString();
        Assert.assertTrue(client.getDoAfterConnectedSuccess());
        Assert.assertEquals(str, expected.toString());
    }


    protected Server startUnpackingServer(int port, String prefix) throws Exception {
        return startServer(port, new IoActionFactory() {

            boolean sended = false;

            @Override
            public IoAction createIoAction(Socket socket) {
                return new AbstractIoAction(socket) {

                    private String line;

                    @Override
                    protected Object doRead(InputStream ins) throws IOException {
                        line = readLine(ins);
                        return line;
                    }

                    @Override
                    protected void doWrite(OutputStream ous, Object readResult) throws IOException {
                        if (prefix != null && !sended) {
                            ous.write(prefix.getBytes());
                            sended = true;
                        }
                        if (!line.contains("CLIENT")) {
                            ous.write(line.getBytes());
                        }
                        sleepIgnoreInterrupt(1);
                        ous.flush();
                    }
                };
            }
        });
    }

    @Test
    public void testSendRequest() throws TimeoutException {
        RedisAsyncNettyClient client = new RedisAsyncNettyClient(b.connect("localhost", server.getPort()),
                new DefaultEndPoint("localhost", server.getPort()), "xpipe", () -> true);
        client.channel().attr(NettyClientHandler.KEY_CLIENT).set(client);

        StringBuffer sb = new StringBuffer();

        StringBuilder expected = new StringBuilder();

        int N = 100;
        runTheTest(client, sb, expected, N, prefix);
        waitConditionUntilTimeOut(()->client.channel().isActive(), 1000);
        sleep(1000);
        String str = sb.toString();
        Assert.assertEquals(str, expected.toString());
    }

    @Test
    public void testFutureClosed() {
        RedisAsyncNettyClient client = new RedisAsyncNettyClient(b.connect("localhost", server.getPort()),
                new DefaultEndPoint("localhost", server.getPort()), "xpipe", () -> true);
        client.channel().attr(NettyClientHandler.KEY_CLIENT).set(client);

        StringBuffer sb = new StringBuffer();

        StringBuilder expected = new StringBuilder();

        int N = 100;
        new Thread(new Runnable() {
            @Override
            public void run() {
                runTheTest(client, sb, expected, N);
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    server.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
        sleep(2 * 1000);
    }


    protected Server startEchoServer(int port, String prefix) throws Exception {
        return startServer(port, new IoActionFactory() {

            @Override
            public IoAction createIoAction(Socket socket) {
                return new AbstractIoAction(socket) {

                    private String line;

                    @Override
                    protected Object doRead(InputStream ins) throws IOException {
                        line = readLine(ins);
                        logger.debug("[doRead]{}", line);
                        logger.info("[doRead]{}", line == null ? null : line.length());
                        return line;
                    }

                    @Override
                    protected void doWrite(OutputStream ous, Object readResult) throws IOException {

                        String[] sp = line.split("\\s+");
                        if (sp.length >= 1) {
                            if (sp[0].equalsIgnoreCase("sleep")) {
                                int sleep = Integer.parseInt(sp[1]);
                                logger.info("[sleep]{}", sleep);
                                sleepIgnoreInterrupt(sleep);
                            }
                        }
                        logger.debug("[doWrite]{}", line.length());
                        logger.debug("[doWrite]{}", line);
                        if (prefix != null) {
                            ous.write(prefix.getBytes());
                        }
                        sleepIgnoreInterrupt(1);
                        ous.write(line.getBytes());
                        ous.flush();
                    }
                };
            }
        });
    }


}
