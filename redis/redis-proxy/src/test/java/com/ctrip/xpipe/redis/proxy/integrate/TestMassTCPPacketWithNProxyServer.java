package com.ctrip.xpipe.redis.proxy.integrate;

import com.ctrip.xpipe.redis.proxy.DefaultProxyServer;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author chen.zhu
 * <p>
 * May 26, 2018
 */
public class TestMassTCPPacketWithNProxyServer extends AbstractProxyIntegrationTest {

    private static final int N_SERVER = 10;

    private DefaultProxyServer server[] = new DefaultProxyServer[N_SERVER];

    private static final String PROXY_HOST = "127.0.0.1";

    private static final int PROXY_PORT[] = new int[N_SERVER];

    @Before
    public void beforeTestMassTCPPacketWithOneProxyServer() throws Exception {
        System.setProperty("server.port", "9992");
        for(int i = 0; i < N_SERVER; i++) {
            PROXY_PORT[i] = randomPort();
            TestProxyConfig config = new TestProxyConfig().setFrontendTcpPort(PROXY_PORT[i]).setFrontendTlsPort(-1);
            server[i] = new DefaultProxyServer().setConfig(config);
            prepare(server[i]);
            server[i].start();
        }

    }

    @After
    public void afterTestMassTCPPacketWithOneProxyServer() throws Exception {
        for(int i = 0; i < N_SERVER; i++) {
            server[i].stop();
        }
    }

    @Test
    public void testStability() throws TimeoutException, InterruptedException {
        int port = randomPort();
        String protocol = generateProxyProtocol(port);
        String message = randomString(100 * 10000);

        ChannelFuture clientFuture = clientBootstrap().connect(PROXY_HOST, PROXY_PORT[0]);

        ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.buffer(1024);
        AtomicReference<ByteBuf> byteBufAtomicReference = new AtomicReference<>(byteBuf);
        ChannelFuture receiveServer = startReceiveServer(port, byteBufAtomicReference);

        String total = protocol + message;
        int index = 2;
        String sendout = total.substring(0, index);
        write(clientFuture, sendout);

        for(int i = 0; i < 2; i++) {
            write(clientFuture, total.substring(index, ++index));
        }

        while(index < total.length()) {
            int pivot;
            do {
                pivot = randomInt(index + 1, total.length() + 1);
            } while(pivot > total.length());
            sendout = total.substring(index, pivot);
            write(clientFuture, sendout);
            index = pivot;
            Thread.sleep(3);
        }
        Thread.sleep(1000 * 10);

        receiveServer.channel().close();

        ByteBuf expected = UnpooledByteBufAllocator.DEFAULT.buffer().writeBytes(message.getBytes());

        waitConditionUntilTimeOut(() -> {
            int rst = ByteBufUtil.compare(expected, byteBufAtomicReference.get());
            logger.info("[testStability] cmp rst: {}", rst);
            return 0 == rst;
        }, 10000, 1000);
        expected.release();
    }

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
            message[i] = randomString(10000);

            clientFuture[i] = clientBootstrap().connect(PROXY_HOST, PROXY_PORT[0]);


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
                    int index = 5;
                    String sendout = total[finalI].substring(0, index);
                    write(clientFuture[finalI], sendout);

                    for (int j = 0; j < 2; j++) {
                        write(clientFuture[finalI], total[finalI].substring(index, ++index));
                    }

                    while (index < total[finalI].length()) {
                        int pivot;
                        do {
                            pivot = randomInt(index + 2, total[finalI].length() + 1);
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

        Thread.sleep(1000 * 60);

        for(int i = 0; i < N; i++) {
            receiveServer[i].channel().close();
            System.out.println(i);
            ByteBuf expected = UnpooledByteBufAllocator.DEFAULT.buffer().writeBytes(message[i].getBytes());

            System.out.println(message[i]);
            System.out.println(references[i].get().toString(Charset.defaultCharset()));
            System.out.println("===========================================");
            Assert.assertEquals(0, ByteBufUtil.compare(expected, references[i].get()));
        }
    }


    private String generateProxyProtocol(int port) {
        StringBuilder sb = new StringBuilder("+PROXY ROUTE ");
        String baseString = "PROXYTCP://127.0.0.1:%d";
        for(int i = 1; i < N_SERVER; i++) {
            sb.append(String.format(baseString, PROXY_PORT[i])).append(" ");
        }
        return String.format(sb.toString() + "TCP://127.0.0.1:%d\r\n", port);

    }
}