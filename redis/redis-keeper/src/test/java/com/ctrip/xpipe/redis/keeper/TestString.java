package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.keeper.handler.CommandHandlerManager;
import com.ctrip.xpipe.redis.keeper.netty.NettyMasterHandler;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.Charset;

/**
 * @author chen.zhu
 * <p>
 * May 03, 2018
 */
public class TestString {
    @Test
    public void testCRLF() {
        String test = "10.1.1.1:80,10.2.2.2:8080 10.3.2.1:90 \r\n";
        String[] splitter = test.split("\\h");
        for(String str : splitter) {
            System.out.println("str: " + str);
            System.out.println(str.equals("\r\n"));
        }
    }

    @Test
    public void testCRLF2() {
        String test = "Proxy Route 10.1.1.1:80,10.2.2.2:8080 10.3.2.1:90\r\nPath 10.2.2.2:8080 10.3.2.1:90\r\n";
        int length = test.getBytes().length;
        test = "$" + length + " " + test;

        String number = test.split("\\h")[0];
        int offset = Integer.parseInt(number.substring(1));
        System.out.println(offset);

        test = test.substring(number.length() + 2);

        String[] infos = test.split("\r\n");
        for(String str : infos) {
            System.out.println("str: " + str);
        }
    }

    @Test
    public void testEncodeDecode() {
        String proxy = "Proxy Route 127.0.0.1:7080,127.0.0.2:8080 10.3.2.1:90";
        RedisClientProtocol protocol = new SimpleStringParser(proxy);
        ByteBuf byteBuf = protocol.format();

        String transferred = byteBuf.toString(Charset.defaultCharset());
        System.out.println(transferred);

        protocol = new SimpleStringParser();
        RedisClientProtocol clientProtocol = protocol.read(byteBuf);
        System.out.println(clientProtocol.getPayload());
    }

    @Test
    public void testCommandHandler() throws Exception {

    }



    @Test
    public void testProxyProtocol() throws IOException, InterruptedException {
        String proxy = "Proxy Route 127.0.0.1:8081,10.2.2.2:80 127.0.0.1:7081,10.5.110.107:7081,10.5.110.107:8090 127.0.0.1:6382";
        RedisClientProtocol protocol = new SimpleStringParser(proxy);
        ByteBuf byteBuf = protocol.format();

        createChannel(6383);
        createChannel(8081);
        createChannel(7081);
        createChannel(6382);

        Thread.sleep(1 * 1000);

        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", 6383));
        socket.getOutputStream().write(byteBuf.array());
        socket.getOutputStream().flush();

        Thread.sleep(10 * 1000);
    }

    private void createChannel(int port) throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap();
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(2);
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LoggingHandler(LogLevel.DEBUG));
                        p.addLast(new NettySimpleMessageHandler());
                        p.addLast(new NettyMasterHandler(null, new CommandHandlerManager(), 1000 * 60 * 24));
                        p.addLast(new HAProxyMessageDecoder());
                    }
                });
        b.bind(port).sync();
    }

    private static final String LINE_SPLITTER = "\\s*;\\s*";
    private static final String ELEMENT_SPLITTER = "\\h";
    private static final String WHITE_SPACE = " ";
    private static final String ARRAY_SPLITTER = "\\s*,\\s*";


    private void handleRequest(Channel channel, Object msg) throws IOException {

        System.out.println(String.format("===================%s===================", channel.localAddress().toString()));
        ByteBuf byteBuf = (ByteBuf) msg;
        RedisClientProtocol parser = new SimpleStringParser().read(byteBuf);
        String protocol = payloadToString(parser.getPayload());

        protocol = protocol.substring("Proxy ".length());
        String[] options = protocol.split(LINE_SPLITTER);
        System.out.println(String.format("********options - %d************", options.length));
        for(String str : options) {
            System.out.println("str: " + str);
        }

        String pathOption = findAndAddToPathOption(options, channel);
        String address = findInRouteOption(options);

        if(address == null) {
            return;
        }
        String routeOption = buildRouteOption(options);
        ByteBuf newProtocol = buildProtocol(routeOption, pathOption);

        connectAndSendProtocol(address, newProtocol);
    }

    private void connectAndSendProtocol(String address, ByteBuf protocol) throws IOException {
        System.out.println("address: " + address);
        String[] hostAndPort = address.split("\\s*:\\s*");
        HostPort hostPort = new HostPort(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(hostPort.getHost(), hostPort.getPort()));
        socket.getOutputStream().write(protocol.array());
        socket.getOutputStream().flush();
    }

    private String findAndAddToPathOption(String[] options, Channel channel) {
        String path = "Path";
        for(String str : options) {
            if(str.contains(path)) {
                path = str;
                break;
            }
        }
        path += WHITE_SPACE + channel.localAddress().toString();
        System.out.println(path);
        return path;
    }

    private String findInRouteOption(String[] options) {
        String route = "Route";
        for(String str : options) {
            if(str.contains(route)) {
                route = str;
                break;
            }
        }
        String[] routes = route.split(ELEMENT_SPLITTER);
        if(routes.length <= 1) {
            System.out.println("===========End=========");
            return null;
        }
        String[] addresses = routes[1].split(ARRAY_SPLITTER);
        for(String str : addresses) {
            if(str.contains("127.0.0.1")) {
                return str;
            }
        }
        return null;
    }

    private String buildRouteOption(String[] options) {
        String route = "Route";
        for(String str : options) {
            if(str.contains(route)) {
                route = str;
                break;
            }
        }
        String[] routes = route.split(ELEMENT_SPLITTER);
        String[] newRoutes = new String[routes.length - 1];
        System.arraycopy(routes, 2, newRoutes, 1, newRoutes.length - 1);
        newRoutes[0] = "Route";

        return StringUtil.join(" ", newRoutes);
    }

    private ByteBuf buildProtocol(String routeOption, String pathOption) {
        String protocol = String.format("Proxy %s;%s", routeOption, pathOption);
        return new SimpleStringParser(protocol).format();
    }

    protected String payloadToString(Object payload) {

        if(payload instanceof String){

            return (String)payload;
        }
        if(payload instanceof ByteArrayOutputStreamPayload){

            ByteArrayOutputStreamPayload baous = (ByteArrayOutputStreamPayload) payload;
            String result = new String(baous.getBytes(), Codec.defaultCharset);
            return result;
        }

        String clazz = payload == null ? "null" : payload.getClass().getSimpleName();
        throw new IllegalStateException(String.format("unknown payload %s:%s", clazz, StringUtil.toString(payload)));
    }
}
