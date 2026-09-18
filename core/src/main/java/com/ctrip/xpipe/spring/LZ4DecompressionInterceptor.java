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

    /**
     * 服务端未携带 Original-Length 时的解压 buffer 上限:压缩后字节数 × 此倍数。
     * 仅作 fallback,正常应走服务端下发的原始长度,避免压缩比超限时解压失败。
     */
    private static final int FALLBACK_COMPRESSION_RATIO = 20;

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

            // 优先用服务端下发的原始长度分配解压 buffer;缺失时退回压缩比经验上限。
            int maxDecompressedLength = resolveMaxDecompressedLength(response, compressed.length);

            LZ4SafeDecompressor decompressor = factory.safeDecompressor();
            byte[] deCompressedData = decompressor.decompress(compressed, maxDecompressedLength);

            // 将解压缩后的数据设置回响应实体
            response.setEntity(new ByteArrayEntity(deCompressedData));

        }
    }

    private int resolveMaxDecompressedLength(HttpResponse response, int compressedLength) {
        Header originalLength = response.getFirstHeader("Original-Length");
        if (originalLength != null) {
            try {
                int len = Integer.parseInt(originalLength.getValue().trim());
                if (len > 0) {
                    return len;
                }
            } catch (NumberFormatException ignored) {
            }
        }
        return compressedLength * FALLBACK_COMPRESSION_RATIO;
    }

}
