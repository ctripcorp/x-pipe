package com.ctrip.xpipe.redis.keeper.spring;


import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperBadRequestException;
import com.ctrip.xpipe.spring.AbstractExceptionHandler;

import org.springframework.http.ResponseEntity;
import org.springframework.web.HttpMediaTypeException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import javax.servlet.http.HttpServletRequest;

import static org.springframework.http.HttpStatus.BAD_REQUEST;

@ControllerAdvice
public class KeeperExceptionHandler extends AbstractExceptionHandler{


    @ExceptionHandler({HttpRequestMethodNotSupportedException.class, HttpMediaTypeException.class,
            RedisKeeperBadRequestException.class})
    public ResponseEntity<Object> badRequest(HttpServletRequest request,
                                                          Throwable ex) {
        return handleError(request, BAD_REQUEST, ex);
    }

}
