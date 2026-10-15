package com.ctrip.xpipe.zk;

import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 测试用 TCP 转发，模拟「静默分区」：{@link #blackhole()} 后连接保持不断，两个方向的数据都被丢弃，
 * 新连接照常 accept 但同样收不到任何响应。与停掉 server 不同，客户端收不到 RST/FIN，
 * 要等读超时才察觉断连。{@link #restore()} 断开现有连接并恢复转发，客户端重连即通。
 */
public class BlackholeTcpProxy implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(BlackholeTcpProxy.class);

    private final int targetPort;
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final ExecutorService executor = Executors.newCachedThreadPool(
            XpipeThreadFactory.create("BlackholeTcpProxy", true));

    private volatile boolean blackhole;
    private volatile boolean closed;
    private ServerSocket server;

    public BlackholeTcpProxy(int targetPort) {
        this.targetPort = targetPort;
    }

    public BlackholeTcpProxy start() throws IOException {
        server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        executor.execute(this::acceptLoop);
        return this;
    }

    public String getConnectString() {
        return InetAddress.getLoopbackAddress().getHostAddress() + ":" + server.getLocalPort();
    }

    public void blackhole() {
        blackhole = true;
        logger.info("[blackhole]");
    }

    /** 断开黑洞期间的连接：其中已被丢弃过数据，字节流不再完整，不能恢复转发。 */
    public void restore() {
        closeSockets();
        blackhole = false;
        logger.info("[restore]");
    }

    @Override
    public void close() throws IOException {
        closed = true;
        try {
            if (null != server) {
                server.close();
            }
        } finally {
            closeSockets();
            executor.shutdownNow();
        }
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket downstream = server.accept();
                Socket upstream = new Socket(InetAddress.getLoopbackAddress(), targetPort);
                sockets.add(downstream);
                sockets.add(upstream);
                executor.execute(() -> pump(downstream, upstream));
                executor.execute(() -> pump(upstream, downstream));
            } catch (IOException e) {
                if (!closed) {
                    logger.info("[acceptLoop]{}", e.toString());
                }
            }
        }
    }

    private void pump(Socket from, Socket to) {
        byte[] buf = new byte[8192];
        try {
            InputStream in = from.getInputStream();
            OutputStream out = to.getOutputStream();
            int n;
            while ((n = in.read(buf)) >= 0) {
                if (!blackhole) {
                    out.write(buf, 0, n);
                    out.flush();
                }
            }
        } catch (IOException e) {
            logger.debug("[pump]{}", e.toString());
        } finally {
            closeQuietly(from);
            closeQuietly(to);
        }
    }

    private void closeSockets() {
        for (Socket socket : sockets) {
            closeQuietly(socket);
        }
        sockets.clear();
    }

    private void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            logger.debug("[closeQuietly]{}", e.toString());
        }
        sockets.remove(socket);
    }
}
