package com.ctrip.xpipe.spring;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.exception.ErrorMessageAware;
import com.ctrip.xpipe.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;

import jakarta.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.MediaType.APPLICATION_JSON;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 5, 2016
 */
public class AbstractExceptionHandler {

    protected Logger logger = LoggerFactory.getLogger(getClass());


    //处理系统内置的Exception
    @ExceptionHandler(Throwable.class)
    public ResponseEntity<Object> exception(HttpServletRequest request, Throwable ex) {
        return handleError(request, INTERNAL_SERVER_ERROR, ex);
    }

    protected ResponseEntity<Object> handleError(HttpServletRequest request, HttpStatus status, Throwable ex) {
        return handleError(request, status, ex, null);
    }

    protected ResponseEntity<Object> handleError(HttpServletRequest request, HttpStatus status,
                                                 Throwable ex, Map<String, String> extraHeaders) {

        String message = ex.getMessage();
        String requestPath = request.getRequestURI() + (request.getQueryString() == null ? "" : ("?" + request
                .getQueryString()));
        if (ExceptionUtils.xpipeExceptionLogMessage(ex)) {
            logger.error("{},{}", message, requestPath);
        } else {
            logger.error(String.format("%s,%s", message, requestPath), ex);
        }

        Object response = null;

        if (ex instanceof ErrorMessageAware) {
            ErrorMessageAware errorMessageAware = (ErrorMessageAware) ex;
            ErrorMessage<?> errorMessage = errorMessageAware.getErrorMessage();
            if (errorMessage != null) {
                response = errorMessage;
            }
        }

        if (response == null) {

            Map<String, Object> errorAttributes = new HashMap<>();
            errorAttributes.put("message", message);
            errorAttributes.put("exception", ex.getClass().getName());

            response = errorAttributes;
        }

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(APPLICATION_JSON);

        if (extraHeaders != null && !extraHeaders.isEmpty()) {
            for (String key : extraHeaders.keySet()) {
                headers.set(key, extraHeaders.get(key));
            }
        }

        return new ResponseEntity<>(response, headers, status);
    }
}
