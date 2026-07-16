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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RedisAsyncNettyClientTest extends AsyncNettyClientTest {

    private static final String prefix = "RedisAsyncNettyClientTest";

    @Before
    public void beforeRedisAsyncNettyClientTestTest() throws Exception{
        server = startEchoServer(randomPort(), prefix);
    }

    @Test
    public void testBusinessWaitsSetName() throws Exception {
        server = startRedisLikeServer(randomPort(), "+OK1\r\n");
        RedisAsyncNettyClient client = new RedisAsyncNettyClient(b.connect("localhost", server.getPort()),
                new DefaultEndPoint("localhost", server.getPort()), "xpipe", () -> true);
        client.channel().attr(NettyClientHandler.KEY_CLIENT).set(client);

        StringBuffer sb = new StringBuffer();
        AtomicBoolean setNameDoneBeforeBusinessReceive = new AtomicBoolean(false);

        String message = "+1" + "\r\n";
        client.sendRequest(Unpooled.copiedBuffer(message.getBytes()), new ByteBufReceiver() {

            private RedisClientProtocol<String> parser = new SimpleStringParser();
            @Override
            public RECEIVER_RESULT receive(Channel channel, ByteBuf byteBuf) {
                setNameDoneBeforeBusinessReceive.set(client.getDoAfterConnectedSuccess());
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
        waitConditionUntilTimeOut(client::getDoAfterConnectedSuccess, 2000);
        waitConditionUntilTimeOut(() -> sb.length() > 0, 2000);
        Assert.assertTrue(client.getDoAfterConnectedOver());
        Assert.assertTrue(setNameDoneBeforeBusinessReceive.get());
        Assert.assertEquals("OK1", sb.toString());
    }

    @Test
    public void testUnpackingSetNameThenBusiness() throws Exception {
        server = startUnpackingServer(randomPort(), "+O");
        RedisAsyncNettyClient client = new RedisAsyncNettyClient(b.connect("localhost", server.getPort()),
                new DefaultEndPoint("localhost", server.getPort()), "xpipe", () -> true);
        client.channel().attr(NettyClientHandler.KEY_CLIENT).set(client);

        StringBuffer sb = new StringBuffer();
        String message = "ping\r\n";
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
        waitConditionUntilTimeOut(client::getDoAfterConnectedSuccess, 2000);
        waitConditionUntilTimeOut(() -> sb.length() > 0, 2000);
        Assert.assertTrue(client.getDoAfterConnectedSuccess());
        Assert.assertEquals("PONG", sb.toString());
    }

    @Test
    public void testCrossThreadBusinessWaitsSetNameOnWire() throws Exception {
        List<String> receivedOrder = Collections.synchronizedList(new ArrayList<>());
        server = startOrderTrackingServer(randomPort(), 300, receivedOrder, "+PONG\r\n");
        RedisAsyncNettyClient client = createClient(server.getPort());

        waitConditionUntilTimeOut(() -> client.channel().isActive(), 2000);

        CountDownLatch businessDone = new CountDownLatch(1);
        AtomicReference<String> businessSendThread = new AtomicReference<>();
        AtomicReference<String> businessReceiveThread = new AtomicReference<>();
        AtomicBoolean businessReceived = new AtomicBoolean(false);

        Thread businessThread = new Thread(() -> {
            businessSendThread.set(Thread.currentThread().getName());
            client.sendRequest(Unpooled.copiedBuffer("ping\r\n".getBytes()), new ByteBufReceiver() {
                private final RedisClientProtocol<String> parser = new SimpleStringParser();

                @Override
                public RECEIVER_RESULT receive(Channel channel, ByteBuf byteBuf) {
                    businessReceiveThread.set(Thread.currentThread().getName());
                    RedisClientProtocol<String> clientProtocol = parser.read(byteBuf);
                    if (clientProtocol != null) {
                        businessReceived.set(true);
                        businessDone.countDown();
                        return RECEIVER_RESULT.SUCCESS;
                    }
                    return RECEIVER_RESULT.CONTINUE;
                }

                @Override
                public void clientClosed(NettyClient nettyClient) {
                    businessDone.countDown();
                }

                @Override
                public void clientClosed(NettyClient nettyClient, Throwable th) {
                    businessDone.countDown();
                }
            });
        }, "business-thread");
        businessThread.start();

        Assert.assertTrue("business should finish after setname", businessDone.await(5, TimeUnit.SECONDS));
        waitConditionUntilTimeOut(client::getDoAfterConnectedSuccess, 2000);

        Assert.assertTrue(receivedOrder.size() >= 2);
        Assert.assertTrue(receivedOrder.get(0).toUpperCase().contains("CLIENT"));
        Assert.assertFalse(receivedOrder.get(1).toUpperCase().contains("CLIENT"));
        Assert.assertTrue(businessReceived.get());
        Assert.assertEquals("business-thread", businessSendThread.get());
        Assert.assertEquals(captureEventLoopThreadName(client), businessReceiveThread.get());
    }

    @Test
    public void testBusinessSendRequestRegistersWhileSetNameInFlight() throws Exception {
        CountDownLatch setNameReceived = new CountDownLatch(1);
        server = startOrderTrackingServer(randomPort(), 500, Collections.synchronizedList(new ArrayList<>()),
                "+PONG\r\n", setNameReceived);
        RedisAsyncNettyClient client = createClient(server.getPort());

        waitConditionUntilTimeOut(() -> client.channel().isActive(), 2000);
        Assert.assertTrue(setNameReceived.await(2, TimeUnit.SECONDS));

        AtomicBoolean queuedWhileSetNameInFlight = new AtomicBoolean(false);
        CountDownLatch businessDone = new CountDownLatch(1);

        Thread businessThread = new Thread(() -> {
            queuedWhileSetNameInFlight.set(!client.getDoAfterConnectedOver());
            client.sendRequest(Unpooled.copiedBuffer("ping\r\n".getBytes()), noopReceiver(businessDone));
        }, "business-thread");
        businessThread.start();

        Assert.assertTrue(businessDone.await(5, TimeUnit.SECONDS));
        waitConditionUntilTimeOut(client::getDoAfterConnectedSuccess, 2000);
        Assert.assertTrue("business sendRequest should be queued before setname completes", queuedWhileSetNameInFlight.get());
    }

    @Test
    public void testMultipleBusinessThreadsAfterSetNameOnWire() throws Exception {
        List<String> receivedOrder = Collections.synchronizedList(new ArrayList<>());
        server = startOrderTrackingServer(randomPort(), 200, receivedOrder, "+PONG\r\n");
        RedisAsyncNettyClient client = createClient(server.getPort());

        waitConditionUntilTimeOut(() -> client.channel().isActive(), 2000);

        int businessThreadCount = 8;
        CountDownLatch allDone = new CountDownLatch(businessThreadCount);
        for (int i = 0; i < businessThreadCount; i++) {
            final int idx = i;
            new Thread(() -> client.sendRequest(Unpooled.copiedBuffer(("ping" + idx + "\r\n").getBytes()), noopReceiver(allDone)),
                    "business-thread-" + idx).start();
        }

        Assert.assertTrue(allDone.await(10, TimeUnit.SECONDS));
        waitConditionUntilTimeOut(client::getDoAfterConnectedSuccess, 2000);

        Assert.assertTrue(receivedOrder.get(0).toUpperCase().contains("CLIENT"));
        for (int i = 1; i < receivedOrder.size(); i++) {
            Assert.assertFalse(receivedOrder.get(i).toUpperCase().contains("CLIENT"));
        }
        Assert.assertEquals(1 + businessThreadCount, receivedOrder.size());
    }

    @Test
    public void testSkipSetNameFromAnotherThread() throws Exception {
        List<String> receivedOrder = Collections.synchronizedList(new ArrayList<>());
        server = startOrderTrackingServer(randomPort(), 0, receivedOrder, "+PONG\r\n");
        RedisAsyncNettyClient client = new RedisAsyncNettyClient(b.connect("localhost", server.getPort()),
                new DefaultEndPoint("localhost", server.getPort()), "xpipe", () -> false);
        client.channel().attr(NettyClientHandler.KEY_CLIENT).set(client);

        CountDownLatch businessDone = new CountDownLatch(1);
        new Thread(() -> client.sendRequest(Unpooled.copiedBuffer("ping\r\n".getBytes()), noopReceiver(businessDone)),
                "business-thread").start();

        Assert.assertTrue(businessDone.await(5, TimeUnit.SECONDS));
        waitConditionUntilTimeOut(client::getDoAfterConnectedSuccess, 2000);
        Assert.assertEquals(1, receivedOrder.size());
        Assert.assertFalse(receivedOrder.get(0).toUpperCase().contains("CLIENT"));
    }

    private RedisAsyncNettyClient createClient(int port) {
        RedisAsyncNettyClient client = new RedisAsyncNettyClient(b.connect("localhost", port),
                new DefaultEndPoint("localhost", port), "xpipe", () -> true);
        client.channel().attr(NettyClientHandler.KEY_CLIENT).set(client);
        return client;
    }

    private String captureEventLoopThreadName(RedisAsyncNettyClient client) throws Exception {
        AtomicReference<String> eventLoopThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        client.channel().eventLoop().execute(() -> {
            eventLoopThread.set(Thread.currentThread().getName());
            latch.countDown();
        });
        Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
        return eventLoopThread.get();
    }

    private ByteBufReceiver noopReceiver(CountDownLatch done) {
        return new ByteBufReceiver() {
            private final RedisClientProtocol<String> parser = new SimpleStringParser();

            @Override
            public RECEIVER_RESULT receive(Channel channel, ByteBuf byteBuf) {
                if (parser.read(byteBuf) != null) {
                    done.countDown();
                    return RECEIVER_RESULT.SUCCESS;
                }
                return RECEIVER_RESULT.CONTINUE;
            }

            @Override
            public void clientClosed(NettyClient nettyClient) {
                done.countDown();
            }

            @Override
            public void clientClosed(NettyClient nettyClient, Throwable th) {
                done.countDown();
            }
        };
    }

    protected Server startOrderTrackingServer(int port, int setNameDelayMs, List<String> receivedOrder,
                                              String businessResponse) throws Exception {
        return startOrderTrackingServer(port, setNameDelayMs, receivedOrder, businessResponse, null);
    }

    protected Server startOrderTrackingServer(int port, int setNameDelayMs, List<String> receivedOrder,
                                              String businessResponse, CountDownLatch setNameReceived) throws Exception {
        return startServer(port, new IoActionFactory() {
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
                        if (line == null) {
                            return;
                        }
                        receivedOrder.add(line);
                        if (line.toUpperCase().contains("CLIENT")) {
                            if (setNameReceived != null) {
                                setNameReceived.countDown();
                            }
                            if (setNameDelayMs > 0) {
                                sleepIgnoreInterrupt(setNameDelayMs);
                            }
                            ous.write("+OK\r\n".getBytes());
                        } else {
                            ous.write(businessResponse.getBytes());
                        }
                        ous.flush();
                    }
                };
            }
        });
    }


    protected Server startUnpackingServer(int port, String firstPart) throws Exception {
        return startServer(port, new IoActionFactory() {

            boolean setNameFirstPartSent = false;

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
                        if (line != null && line.toUpperCase().contains("CLIENT")) {
                            if (!setNameFirstPartSent) {
                                ous.write(firstPart.getBytes());
                                ous.flush();
                                setNameFirstPartSent = true;
                                sleepIgnoreInterrupt(5);
                                ous.write("K\r\n".getBytes());
                                ous.flush();
                            } else {
                                ous.write("+OK\r\n".getBytes());
                                ous.flush();
                            }
                            return;
                        }
                        ous.write("+PONG\r\n".getBytes());
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

        int N = 100;
        AtomicInteger responseCount = new AtomicInteger(0);
        for (int i = 0; i < N; i++) {
            String message = "+" + i + "\r\n";
            client.sendRequest(Unpooled.copiedBuffer(message.getBytes()), new ByteBufReceiver() {
                private final RedisClientProtocol<String> parser = new SimpleStringParser();

                @Override
                public RECEIVER_RESULT receive(Channel channel, ByteBuf byteBuf) {
                    if (parser.read(byteBuf) != null) {
                        responseCount.incrementAndGet();
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
        }
        waitConditionUntilTimeOut(client::getDoAfterConnectedSuccess, 2000);
        waitConditionUntilTimeOut(() -> responseCount.get() == N, 5000);
        Assert.assertEquals(N, responseCount.get());
        Assert.assertTrue(client.getDoAfterConnectedSuccess());
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

    /**
     * Respond +OK to CLIENT SETNAME; otherwise write the given business response once per request.
     */
    protected Server startRedisLikeServer(int port, String businessResponse) throws Exception {
        return startServer(port, new IoActionFactory() {

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
                        if (line != null && line.toUpperCase().contains("CLIENT")) {
                            ous.write("+OK\r\n".getBytes());
                        } else {
                            ous.write(businessResponse.getBytes());
                        }
                        ous.flush();
                    }
                };
            }
        });
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

                        if (line != null && line.toUpperCase().contains("CLIENT")) {
                            ous.write("+OK\r\n".getBytes());
                            ous.flush();
                            return;
                        }

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
