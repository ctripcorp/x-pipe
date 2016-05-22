package com.ctrip.xpipe.redis.simple.latency;

import java.net.InetSocketAddress;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * psync output commandstream, cal latency
 * @author wenchao.meng
 *
 * May 22, 2016 2:43:51 PM
 */
public class PsyncLatencyTest extends AbstractLifecycle{
	
	private InetSocketAddress master = new InetSocketAddress("127.0.0.1", 6379);

//	private InetSocketAddress dest = new InetSocketAddress("127.0.0.1", 6479);

	private InetSocketAddress dest = new InetSocketAddress("127.0.0.1", 7777);

	private String runId = "?";
	
	private long offset = -1;


	public static void main(String[] args) throws Exception {
		
		new PsyncLatencyTest().start();
	}
	
	@Override
	protected void doStart() throws Exception {
		startSendMessage();
		startGetLatency();
	}

	private void startGetLatency() {

		EventLoopGroup eventLoopGroup = new NioEventLoopGroup(); 
        Bootstrap b = new Bootstrap();
        b.group(eventLoopGroup)
         .channel(NioSocketChannel.class)
         .option(ChannelOption.TCP_NODELAY, true)
         .handler(new ChannelInitializer<SocketChannel>() {
             @Override
             public void initChannel(SocketChannel ch) throws Exception {
                 ChannelPipeline p = ch.pipeline();
                 p.addLast(new LoggingHandler(LogLevel.DEBUG));
                 p.addLast(new NettySimpleMessageHandler());
                 p.addLast(new ReceiveMessageHandler(runId, offset));
             }
         });
        b.connect(dest);
	}

	private void startSendMessage() {

		EventLoopGroup eventLoopGroup = new NioEventLoopGroup(); 
        Bootstrap b = new Bootstrap();
        b.group(eventLoopGroup)
         .channel(NioSocketChannel.class)
         .option(ChannelOption.TCP_NODELAY, true)
         .handler(new ChannelInitializer<SocketChannel>() {
             @Override
             public void initChannel(SocketChannel ch) throws Exception {
                 ChannelPipeline p = ch.pipeline();
                 p.addLast(new LoggingHandler(LogLevel.DEBUG));
                 p.addLast(new NettySimpleMessageHandler());
                 p.addLast(new SetMessageHandler());
             }
         });
        
        b.connect(master);
	}

}
