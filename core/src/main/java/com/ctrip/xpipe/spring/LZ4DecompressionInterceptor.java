package com.ctrip.xpipe.spring;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.protocol.HttpContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class LZ4DecompressionInterceptor implements HttpResponseInterceptor {

    private static LZ4Factory factory = LZ4Factory.fastestInstance();

    @Override
    public void process(HttpResponse response, HttpContext context) throws HttpException, IOException {
        Header head = response.getFirstHeader("Content-Encoding");
        if (head == null) {
            return;
        }
        String encoding = head.getValue();
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

            // 将解压缩后的数据设置回响应实体
            response.setEntity(new ByteArrayEntity(deCompressedData));

        }
    }

}
