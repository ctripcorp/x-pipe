package com.ctrip.framework.xpipe.redis.utils;

import com.alibaba.arthas.deps.org.slf4j.Logger;
import com.alibaba.arthas.deps.org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionUtil {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionUtil.class);

    public static Map<SocketChannel, Lock> socketChannelMap = new ConcurrentHashMap<>();

    public static InetSocketAddress getAddress(Object o, InetSocketAddress socketAddress) {
        if (ProxyUtil.getInstance().needProxy(socketAddress)) {
            InetSocketAddress proxy =  ProxyUtil.getInstance().getProxyAddress(o, socketAddress);
            logger.info("[Destination -> Proxy]: {} -> {}", socketAddress, proxy);
            return proxy;
        } else {
            return socketAddress;
        }
    }

    public static SocketAddress getAddress(Object o, SocketAddress socketAddress) {
        return getAddress(o, (InetSocketAddress) socketAddress);
    }

    public static SocketAddress removeAddress(Object o) {
        logger.info("[SocketAddress] removed for {}", o);
        return ProxyUtil.getInstance().removeProxyAddress(o);
    }

    public static void connectToProxy(Socket socket, InetSocketAddress address, int timeout) throws IOException {
        socket.connect(address, timeout);
        byte[] bytes = ProxyUtil.getInstance().getProxyConnectProtocol(socket);
        socket.getOutputStream().write(bytes);
        socket.getOutputStream().flush();
        logger.info("[Connect] to {} -> {} with protocol {}", socket.getLocalSocketAddress(), address, new String(bytes));
    }

    public static boolean connectToProxy(SocketChannel socketChannel, SocketAddress address) throws IOException {
        socketChannelMap.put(socketChannel, new ReentrantLock());
        logger.info("[Connect] to proxy {} through Netty SocketChannel", address);
        return socketChannel.connect(address);
    }

    public static String getString(SocketAddress address) {
        return address.toString();
    }

    /**
     * send protocol in first write
     * @param socketChannel
     * @throws IOException
     */
    public static void sendProtocolToProxy(SocketChannel socketChannel) throws IOException {

        Lock lock = socketChannelMap.get(socketChannel);
        if (lock == null) {
            return;
        }
        try {
            lock.lock();
            Lock l = socketChannelMap.get(socketChannel);
            if (l != null) {
                socketChannelMap.remove(socketChannel);
                ByteBuffer byteBuffer = ByteBuffer.allocate(512);
                byte[] bytes = ProxyUtil.getInstance().getProxyConnectProtocol(socketChannel);
                byteBuffer.put(bytes);
                byteBuffer.flip();
                socketChannel.write(byteBuffer);
                byteBuffer.clear();
                logger.info("[Proxy] sends protocol {} to {} -> {}", new String(bytes), socketChannel.getLocalAddress(), socketChannel.getRemoteAddress());
            }
        } finally {
            lock.unlock();
        }
    }

}
