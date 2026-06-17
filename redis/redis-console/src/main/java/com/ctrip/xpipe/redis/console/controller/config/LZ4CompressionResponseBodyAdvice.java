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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

@RestControllerAdvice
public class LZ4CompressionResponseBodyAdvice implements ResponseBodyAdvice<Object> {

    private static LZ4Factory factory = LZ4Factory.fastestInstance();

    @Override
    public boolean supports(MethodParameter methodParameter, Class<? extends HttpMessageConverter<?>> aClass) {

        Class<?> returnType = methodParameter.getParameterType();
        if (returnType != byte[].class && returnType != String.class) {
            return false;
        }
        HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
        String encode = request.getHeader(HttpHeaders.ACCEPT_ENCODING);
        return encode != null && encode.contains("lz4");
    }

    @Override
    public byte[] beforeBodyWrite(Object body, MethodParameter methodParameter, MediaType mediaType, Class<? extends HttpMessageConverter<?>> aClass, ServerHttpRequest serverHttpRequest, ServerHttpResponse serverHttpResponse) {

        byte[] bytes;
        if (body instanceof String) {
            bytes = ((String) body).getBytes(StandardCharsets.UTF_8);
        } else {
            bytes = (byte[]) body;
        }

        LZ4Compressor compressor = factory.fastCompressor();
        int maxCompressedLength = compressor.maxCompressedLength(bytes.length);
        byte[] compressedBytes = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(bytes, 0, bytes.length, compressedBytes, 0, maxCompressedLength);

        serverHttpResponse.getHeaders().set("Content-Encoding", "lz4");
        serverHttpResponse.getHeaders().setContentLength(compressedLength);

        return Arrays.copyOf(compressedBytes, compressedLength);
    }
}
