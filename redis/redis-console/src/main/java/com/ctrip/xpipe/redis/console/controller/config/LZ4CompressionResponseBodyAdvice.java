package com.ctrip.xpipe.redis.console.controller.config;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;

import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;

@RestControllerAdvice
public class LZ4CompressionResponseBodyAdvice implements ResponseBodyAdvice<byte[]> {

    private static LZ4Factory factory = LZ4Factory.fastestInstance();

    @Override
    public boolean supports(MethodParameter methodParameter, Class<? extends HttpMessageConverter<?>> aClass) {

        if(!methodParameter.getParameterType().equals(byte[].class)) {
            return false;
        }
        // 获取请求头
        HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
        String encode = request.getHeader(HttpHeaders.ACCEPT_ENCODING);
        // 检查请求头中是否包含 Content-Encoding: lz4
        return encode!= null && encode.contains("lz4");
    }

    @Override
    public byte[] beforeBodyWrite(byte[] body, MethodParameter methodParameter, MediaType mediaType, Class<? extends HttpMessageConverter<?>> aClass, ServerHttpRequest serverHttpRequest, ServerHttpResponse serverHttpResponse) {

        LZ4Compressor compressor = factory.fastCompressor();

        // 压缩字节数组
        int maxCompressedLength = compressor.maxCompressedLength(body.length);
        byte[] compressedBytes = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(body, 0, body.length, compressedBytes, 0, maxCompressedLength);

        // 设置 Content-Encoding 头部为 lz4
        serverHttpResponse.getHeaders().set("Content-Encoding", "lz4");
        serverHttpResponse.getHeaders().setContentLength(compressedLength);

        // 返回压缩后的字节数组（需要截取实际压缩长度）
        return Arrays.copyOf(compressedBytes, compressedLength);

    }
}
