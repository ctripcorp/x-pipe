package com.ctrip.xpipe.redis.proxy.integrate;

import com.ctrip.xpipe.redis.proxy.DefaultProxyServer;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import org.junit.*;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author chen.zhu
 * <p>
 * May 23, 2018
 */
public class TestMassTCPPacketWithOneProxyServer extends AbstractProxyIntegrationTest {

    private DefaultProxyServer server;

    private static final String PROXY_HOST = "127.0.0.1";

    private static final int PROXY_PORT = randomPort();

    @Before
    public void beforeTestMassTCPPacketWithOneProxyServer() throws Exception {
        System.setProperty("server.port", "9992");
        server = new DefaultProxyServer().setConfig(new TestProxyConfig().setFrontendTlsPort(-1).setFrontendTcpPort(PROXY_PORT));
        prepare(server);
        server.start();
    }

    @After
    public void afterTestMassTCPPacketWithOneProxyServer() throws Exception {
        server.stop();
        System.gc();
        sleep(1000 * 2);
    }

    @Test
    public void testStability() throws TimeoutException, InterruptedException {
        int port = randomPort();
        String protocol = generateProxyProtocol(port);
        String message = randomString(10 * 10000);

        ChannelFuture clientFuture = clientBootstrap().connect(PROXY_HOST, PROXY_PORT);

        ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.buffer(1024);
        AtomicReference<ByteBuf> byteBufAtomicReference = new AtomicReference<>(byteBuf);
        ChannelFuture receiveServer = startReceiveServer(port, byteBufAtomicReference);

        String total = protocol + message;
        int index = 3;
        String sendout = total.substring(0, index);
        write(clientFuture, sendout);

        for(int i = 0; i < 2; i++) {
            write(clientFuture, total.substring(index, ++index));
        }

        while(index < total.length()) {
            int pivot = index + 1;
            do {
                pivot = randomInt(index + 1, total.length() + 1);
            } while(pivot > total.length());
            sendout = total.substring(index, pivot);
            write(clientFuture, sendout);
            index = pivot;
            Thread.sleep(5);
        }
        Thread.sleep(1000 * 1);

        receiveServer.channel().close();

        ByteBuf expected = UnpooledByteBufAllocator.DEFAULT.buffer().writeBytes(message.getBytes());

        waitConditionUntilTimeOut(() -> {
            int rst = ByteBufUtil.compare(expected, byteBufAtomicReference.get());
            logger.info("[testStability] cmp rst: {}", rst);
            return 0 == rst;
        }, 10000, 1000);
        expected.release();
    }

    @Ignore
    @Test
    public void testStabilityWithTwo() throws TimeoutException, InterruptedException {
        int port1 = randomPort(), port2 = randomPort();
        String protocol1 = generateProxyProtocol(port1);
        String protocol2 = generateProxyProtocol(port2);

        String message1 = randomString(10 * 10000);
        String message2 = randomString(10 * 10000);

        ChannelFuture clientFuture1 = clientBootstrap().connect(PROXY_HOST, PROXY_PORT);
        ChannelFuture clientFuture2 = clientBootstrap().connect(PROXY_HOST, PROXY_PORT);

        ByteBuf byteBuf1 = UnpooledByteBufAllocator.DEFAULT.buffer(1024);
        AtomicReference<ByteBuf> byteBufAtomicReference1 = new AtomicReference<>(byteBuf1);
        ChannelFuture receiveServer1 = startReceiveServer(port1, byteBufAtomicReference1);

        ByteBuf byteBuf2 = UnpooledByteBufAllocator.DEFAULT.buffer(1024);
        AtomicReference<ByteBuf> byteBufAtomicReference2 = new AtomicReference<>(byteBuf2);
        ChannelFuture receiveServer2 = startReceiveServer(port2, byteBufAtomicReference2);

        String total1 = protocol1 + message1;
        String total2 = protocol2 + message2;

        new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        int index = 3;
                        String sendout = total1.substring(0, index);
                        write(clientFuture1, sendout);

                        for(int i = 0; i < 2; i++) {
                            write(clientFuture1, total1.substring(index, ++index));
                        }

                        while(index < total1.length()) {
                            int pivot = index + 1;
                            do {
                                pivot = randomInt(index + 1, total1.length() + 1);
                            } while(pivot > total1.length());
                            sendout = total1.substring(index, pivot);
                            write(clientFuture1, sendout);
                            index = pivot;
                            try {
                                Thread.sleep(5);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
        ).start();

        new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        int index = 3;
                        String sendout = total2.substring(0, index);
                        write(clientFuture2, sendout);

                        for(int i = 0; i < 2; i++) {
                            write(clientFuture2, total2.substring(index, ++index));
                        }

                        while(index < total2.length()) {
                            int pivot;
                            do {
                                pivot = randomInt(index + 1, total2.length() + 1);
                            } while(pivot > total2.length());
                            sendout = total2.substring(index, pivot);
                            write(clientFuture2, sendout);
                            index = pivot;
                            try {
                                Thread.sleep(5);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
        ).start();

        Thread.sleep(1000 * 3);

        receiveServer1.channel().close();
        receiveServer2.channel().close();

        ByteBuf expected = UnpooledByteBufAllocator.DEFAULT.buffer().writeBytes(message1.getBytes());

        Assert.assertEquals(0, ByteBufUtil.compare(expected, byteBufAtomicReference1.get()));

        expected = UnpooledByteBufAllocator.DEFAULT.buffer().writeBytes(message2.getBytes());

        Assert.assertEquals(0, ByteBufUtil.compare(expected, byteBufAtomicReference2.get()));

        expected.release();
    }

    @Ignore
    @Test
    public void testStabilityWithN() throws TimeoutException, InterruptedException {
        int N = 100;
        int[] port = new int[N];
        String[] protocol = new String[N], message = new String[N], total = new String[N];
        AtomicReference<ByteBuf>[] references = new AtomicReference[N];
        ChannelFuture[] clientFuture = new ChannelFuture[N], receiveServer = new ChannelFuture[N];

        for(int i = 0; i < N; i++) {
            port[i] = randomPort();
            protocol[i] = generateProxyProtocol(port[i]);
            message[i] = randomString(1000);

            clientFuture[i] = clientBootstrap().connect(PROXY_HOST, PROXY_PORT);

            ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.buffer(1024);
            references[i] = new AtomicReference<>(byteBuf);
            receiveServer[i] = startReceiveServer(port[i], references[i]);

            total[i] = protocol[i] + message[i];
        }

        AtomicInteger counter = new AtomicInteger(0);
        for(int i = 0; i < N; i++) {
            int finalI = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    int index = 3;
                    String sendout = total[finalI].substring(0, index);
                    write(clientFuture[finalI], sendout);

                    for (int j = 0; j < 2; j++) {
                        write(clientFuture[finalI], total[finalI].substring(index, ++index));
                    }

                    while (index < total[finalI].length()) {
                        int pivot = index + 1;
                        do {
                            pivot = randomInt(index + 1, total[finalI].length() + 1);
                        } while (pivot > total[finalI].length());
                        sendout = total[finalI].substring(index, pivot);
                        write(clientFuture[finalI], sendout);
                        index = pivot;
                        try {
                            Thread.sleep(5);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    counter.getAndIncrement();
                }
            }).start();

        }

        Thread.sleep(1000 * N/100 + 1);
        waitConditionUntilTimeOut(()-> {
            return counter.get() == N;
        }, 10000);

        for(int i = 0; i < N; i++) {
            receiveServer[i].channel().close();

            ByteBuf expected = UnpooledByteBufAllocator.DEFAULT.buffer().writeBytes(message[i].getBytes());

            logger.info("[testStabilityWithN] count: {}", i);
            Assert.assertEquals(0, ByteBufUtil.compare(expected, references[i].get()));
            expected.release();
        }
    }


    private String generateProxyProtocol(int port) {
        return String.format("+PROXY ROUTE TCP://127.0.0.1:%d;FORWARD_FOR 127.0.0.1:80\r\n", port);
    }

}
