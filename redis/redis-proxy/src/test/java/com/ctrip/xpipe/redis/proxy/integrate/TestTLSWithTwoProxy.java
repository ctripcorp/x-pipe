package com.ctrip.xpipe.redis.proxy.integrate;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.proxy.DefaultProxyServer;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import org.junit.*;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author chen.zhu
 * <p>
 * May 26, 2018
 */

public class TestTLSWithTwoProxy extends AbstractProxyIntegrationTest {

    private DefaultProxyServer server1, server2;

    private static final String PROXY_HOST = "127.0.0.1";

    private static final int PROXY_PORT1 = randomPort(), PROXY_PORT2 = randomPort();

    @Before
    public void beforeTestMassTCPPacketWithOneProxyServer() throws Exception {
        System.setProperty("server.port", "9992");
        server1 = new DefaultProxyServer().setConfig(new TestProxyConfig().setFrontendTlsPort(-1).setFrontendTcpPort(PROXY_PORT1));
        prepare(server1);
        server1.start();

        server2 = new DefaultProxyServer().setConfig(new TestProxyConfig().setFrontendTlsPort(PROXY_PORT2).setFrontendTcpPort(randomPort()));
        prepareTLS(server2);
        server2.start();
    }

    @After
    public void afterTestMassTCPPacketWithOneProxyServer() throws Exception {
        server1.stop();
        server2.stop();
        System.gc();
        System.gc();
        Thread.sleep(2000);
    }

    @Test
    public void testStability() throws TimeoutException, InterruptedException {
        int port = randomPort();
        String protocol = generateProxyProtocol(port);
        String message = randomString(10000);

        ChannelFuture clientFuture = clientBootstrap().connect(PROXY_HOST, PROXY_PORT1);

        ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.buffer(message.getBytes().length);
        AtomicReference<ByteBuf> byteBufAtomicReference = new AtomicReference<>(byteBuf);
        ChannelFuture receiveServer = startReceiveServer(port, byteBufAtomicReference);

        final String total = protocol + message;
        int index = 2;
        String sendout = total.substring(0, index);
        waitConditionUntilTimeOut(()->clientFuture.channel().isActive(), 100);
        write(clientFuture, sendout);

        for(int i = 0; i < 2; i++) {
            write(clientFuture, total.substring(index, ++index));
        }

        while(index < total.length()) {
            int pivot = 0;
            do {
                pivot = randomInt(index + 1, total.length() + 1);
            } while(pivot > total.length());
            sendout = total.substring(index, pivot);
            write(clientFuture, sendout);
            index = pivot;
            Thread.sleep(5);
        }
        Thread.sleep(1000 * 2);

        receiveServer.channel().close();

        ByteBuf expected = Unpooled.wrappedBuffer(message.getBytes());

        waitConditionUntilTimeOut(() -> {
            int rst = ByteBufUtil.compare(expected, byteBufAtomicReference.get());
            logger.info("[testStability] cmp rst: {}", rst);
            return 0 == rst;
        }, 10000, 1000);
        expected.release();
    }

    @Test
    public void testStabilityWithCompressAndSSL() throws TimeoutException, InterruptedException {
        ((TestProxyConfig)server1.getConfig()).setCompress(true);
        ((TestProxyConfig)server1.getResourceManager().getProxyConfig()).setCompress(true);
        testStability();
    }

    @Ignore
    @Test
    public void testMultiThreadStabilityWithCompressAndSSL() throws Exception {
        int N = 20;

        ((TestProxyConfig)server1.getConfig()).setCompress(true);
        ((TestProxyConfig)server1.getResourceManager().getProxyConfig()).setCompress(true);

        List<String> samples = Lists.newArrayList();
        List<AtomicReference<ByteBuf>> result = Lists.newArrayList();
        List<String> protocols = Lists.newArrayList();

        for(int i = 0; i < N; i++) {

            String message = randomString(1024 * 10);
            samples.add(message);
            ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.ioBuffer(1024 * 10);
            result.add(new AtomicReference<>(byteBuf));
        }

        for(int i = 0; i < N; i++) {
            int port = randomPort();
            String protocol = generateProxyProtocol(port);
            protocols.add(protocol);
            startReceiveServer(port, result.get(i));
        }

        CountDownLatch latch = new CountDownLatch(N);
        for(int i = 0; i < N; i++) {
            final String total = protocols.get(i) + samples.get(i);

            new Thread(new AbstractExceptionLogTask() {
                @Override
                public void doRun() throws Exception{
                    ChannelFuture clientFuture = clientBootstrap().connect(PROXY_HOST, PROXY_PORT1);
                    int index = 2;
                    String sendout = total.substring(0, index);
                    waitConditionUntilTimeOut(()->clientFuture.channel().isActive(), 200);
                    write(clientFuture, sendout);

                    for(int i = 0; i < 2; i++) {
                        write(clientFuture, total.substring(index, ++index));
                    }

                    while(index < total.length()) {
                        int pivot = 0;
                        do {
                            pivot = randomInt(index + 1, total.length() + 1);
                        } while(pivot > total.length());
                        sendout = total.substring(index, pivot);
                        write(clientFuture, sendout);
                        index = pivot;
                        Thread.sleep(5);
                    }
                    latch.countDown();
                }
            }).start();
        }

        latch.await();
        sleep(10 * 1000);

        for(int i = 0; i < N; i++) {
            Assert.assertEquals(samples.get(i), result.get(i).get().toString(Charset.defaultCharset()));
            result.get(i).get().release();
        }
    }

    @Ignore
    @Test
    //Manullay test
    public void testStabilityWithN() throws TimeoutException, InterruptedException {
        int N = 50;
        int[] port = new int[N];
        String[] protocol = new String[N], message = new String[N], total = new String[N];
        AtomicReference<ByteBuf>[] references = new AtomicReference[N];
        ChannelFuture[] clientFuture = new ChannelFuture[N], receiveServer = new ChannelFuture[N];

        for(int i = 0; i < N; i++) {
            port[i] = randomPort();
            protocol[i] = generateProxyProtocol(port[i]);
            message[i] = randomString(1000);

            clientFuture[i] = clientBootstrap().connect(PROXY_HOST, PROXY_PORT1);


            ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.buffer(1024);
            references[i] = new AtomicReference<>(byteBuf);
            receiveServer[i] = startReceiveServer(port[i], references[i]);

            total[i] = protocol[i] + message[i];
        }

        for(int i = 0; i < N; i++) {
            int finalI = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    int index = 2;
                    String sendout = total[finalI].substring(0, index);
                    write(clientFuture[finalI], sendout);

                    for (int j = 0; j < 2; j++) {
                        write(clientFuture[finalI], total[finalI].substring(index, ++index));
                    }

                    while (index < total[finalI].length()) {
                        int pivot = 0;
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
                }
            }).start();

        }

        Thread.sleep(1500 * N/100 + 2000);

        for(int i = 0; i < N; i++) {
            receiveServer[i].channel().close();

            ByteBuf expected = UnpooledByteBufAllocator.DEFAULT.buffer().writeBytes(message[i].getBytes());

            Assert.assertEquals(0, ByteBufUtil.compare(expected, references[i].get()));
        }
    }

    private String generateProxyProtocol(int port) {
        return String.format("+PROXY ROUTE PROXYTLS://127.0.0.1:%d TCP://127.0.0.1:%d;FORWARD_FOR 127.0.0.1:80\r\n", PROXY_PORT2, port);
    }
}