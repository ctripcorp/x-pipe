package com.ctrip.xpipe.redis.keeper.spring;


import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerErrorParser;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;
import com.ctrip.xpipe.spring.AbstractExceptionHandler;
import com.google.common.collect.ImmutableMap;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.HttpMediaTypeException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.util.Map;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;

@ControllerAdvice
public class KeeperExceptionHandler extends AbstractExceptionHandler {

    @ExceptionHandler(RedisKeeperRuntimeException.class)
    public ResponseEntity<Object> exception(HttpServletRequest request, RedisKeeperRuntimeException ex) {
        Map<String, String> headers = ImmutableMap.of(KeeperContainerErrorParser.ERROR_HEADER_NAME, ex
                .getErrorMessage().getErrorType().toString());

        return handleError(request, INTERNAL_SERVER_ERROR, ex, headers);
    }

    @ExceptionHandler({HttpRequestMethodNotSupportedException.class, HttpMediaTypeException.class})
    public ResponseEntity<Object> badRequest(HttpServletRequest request,
                                             Throwable ex) {
        return handleError(request, BAD_REQUEST, ex);
    }

}
