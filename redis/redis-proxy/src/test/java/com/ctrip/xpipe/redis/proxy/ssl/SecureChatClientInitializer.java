package com.ctrip.xpipe.redis.proxy.ssl;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;

/**
 * @author chen.zhu
 * <p>
 * May 08, 2018
 */
public class SecureChatClientInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        // Add SSL handler first to encrypt and decrypt everything.
        // In this example, we use a bogus certificate in the server side
        // and accept any invalid certificates in the client side.
        // You will need something more complicated to identify both
        // and server in the real world.
        pipeline.addLast(createSslHandler(getClientSSLContext()));

        // On top of the SSL handler, add the text line codec.
        pipeline.addLast(new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
        pipeline.addLast(new StringDecoder());
        pipeline.addLast(new StringEncoder());

        // and then business logic.
        pipeline.addLast(new SecureChatClientHandler());
    }

    private ChannelHandler createSslHandler(SSLContext sslContext) {
        SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(true);
        return new SslHandler(sslEngine);
    }
    public SSLContext getClientSSLContext() throws Exception {
        KeyStore trustKeyStore= KeyStore.getInstance("JKS");// 访问Java密钥库，JKS是keytool创建的Java密钥库
        InputStream keyStream = new FileInputStream("/opt/cert/cChat.jks");//打开证书文件（.jks格式）
        char keyStorePass[]="123456".toCharArray();  //证书密码
        trustKeyStore.load(keyStream,keyStorePass);


        TrustManagerFactory trustManagerFactory =   TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustKeyStore );//保存服务端的授权证书

        SSLContext   clientContext = SSLContext.getInstance( "TLS");
        clientContext.init(null, trustManagerFactory.getTrustManagers(), null);

        return clientContext;
    }
}
