package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperBadRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.HttpMediaTypeException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.MediaType.APPLICATION_JSON;

@ControllerAdvice
public class GlobalDefaultExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(GlobalDefaultExceptionHandler.class);

    //处理系统内置的Exception
    @ExceptionHandler(Throwable.class)
    public ResponseEntity<Map<String, Object>> exception(HttpServletRequest request, Throwable ex) {
        return handleError(request, INTERNAL_SERVER_ERROR, ex);
    }

    @ExceptionHandler({HttpRequestMethodNotSupportedException.class, HttpMediaTypeException.class,
            RedisKeeperBadRequestException.class})
    public ResponseEntity<Map<String, Object>> badRequest(HttpServletRequest request,
                                                          Throwable ex) {
        return handleError(request, BAD_REQUEST, ex);
    }

    private ResponseEntity<Map<String, Object>> handleError(HttpServletRequest request,
                                                            HttpStatus status, Throwable ex) {
        String message = ex.getMessage();

        logger.error(message, ex);

        Map<String, Object> errorAttributes = new HashMap<>();

        errorAttributes.put("status", status.value());
        errorAttributes.put("message", message);
        errorAttributes.put("exception", ex.getClass().getName());

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(APPLICATION_JSON);
        return new ResponseEntity<>(errorAttributes, headers, status);
    }

}
