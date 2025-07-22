package com.ctrip.xpipe.spring;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.hc.client5.http.classic.ExecChain;
import org.apache.hc.client5.http.classic.ExecChainHandler;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class Lz4DecompressExecChainHandler implements ExecChainHandler {

    private static final LZ4Factory factory = LZ4Factory.fastestInstance();
    private static final Logger log = LoggerFactory.getLogger(Lz4DecompressExecChainHandler.class);

    @Override
    public ClassicHttpResponse execute(ClassicHttpRequest classicHttpRequest, ExecChain.Scope scope, ExecChain execChain) throws IOException, HttpException {
        ClassicHttpResponse response = execChain.proceed(classicHttpRequest, scope);
        String encoding = response.getFirstHeader("Content-Encoding") != null ?
                response.getFirstHeader("Content-Encoding").getValue() : null;

        if ("lz4".equalsIgnoreCase(encoding)) {
            // 获取响应实体
            InputStream entityStream = response.getEntity().getContent();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = entityStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }

            byte[] compressed = outputStream.toByteArray();

            LZ4SafeDecompressor decompressor = factory.safeDecompressor();
            byte[] deCompressedData = decompressor.decompress(compressed, compressed.length * 20);

            // 直接获取内容类型，不使用原始内容类型字符串
            // 改用默认的安全内容类型，避开解析错误
            ContentType contentType = ContentType.APPLICATION_OCTET_STREAM;

            // 将解压缩后的数据设置回响应实体
            response.setEntity(new ByteArrayEntity(deCompressedData, contentType));
            response.removeHeaders("Content-Encoding");
            response.removeHeaders("Content-Length");
            response.addHeader("Content-Length", String.valueOf(deCompressedData.length));

        }
        return response;
    }
}