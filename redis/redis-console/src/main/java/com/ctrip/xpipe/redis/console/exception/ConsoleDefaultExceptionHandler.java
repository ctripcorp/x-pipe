package com.ctrip.xpipe.redis.console.exception;

import com.ctrip.xpipe.spring.AbstractExceptionHandler;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.HttpMediaTypeException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.dal.jdbc.DalRuntimeException;

/**
 * @author shyin
 *
 * Aug 9, 2016
 */

@ControllerAdvice
public class ConsoleDefaultExceptionHandler extends AbstractExceptionHandler {

	@ExceptionHandler(DalNotFoundException.class)
	public ResponseEntity<Object> dalNotFound(HttpServletRequest request, DalNotFoundException ex) {
		return handleError(request,HttpStatus.NOT_FOUND, ex);
	}
	
	@ExceptionHandler(DalRuntimeException.class)
	public ResponseEntity<Object> dalRT(HttpServletRequest request, DalRuntimeException ex) {
		return handleError(request,HttpStatus.INTERNAL_SERVER_ERROR, ex);
	}
	
	@ExceptionHandler(DataNotFoundException.class)
	public ResponseEntity<Object> dataNotFound(HttpServletRequest request, DataNotFoundException ex) {
		return handleError(request,HttpStatus.NOT_FOUND, ex);
	}
	
	@ExceptionHandler(BadRequestException.class)
	public ResponseEntity<Object> badUserRequest(HttpServletRequest request, BadRequestException ex) {
		return handleError(request,HttpStatus.BAD_REQUEST, ex);
	}
	
	@ExceptionHandler({HttpRequestMethodNotSupportedException.class, HttpMediaTypeException.class})
	public ResponseEntity<Object> badRequest(HttpServletRequest request, ServletException ex) {
	    return handleError(request, HttpStatus.BAD_REQUEST, ex);
	  }

}
