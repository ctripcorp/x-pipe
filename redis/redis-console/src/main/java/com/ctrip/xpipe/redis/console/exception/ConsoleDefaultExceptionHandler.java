package com.ctrip.xpipe.redis.console.exception;

import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.HttpMediaTypeException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.dal.jdbc.DalRuntimeException;

import com.ctrip.xpipe.spring.AbstractExceptionHandler;

/**
 * @author shyin
 *
 * Aug 9, 2016
 */

@ControllerAdvice
public class ConsoleDefaultExceptionHandler extends AbstractExceptionHandler {

	// Dal Not Found Exception
	@ExceptionHandler(DalNotFoundException.class)
	public ResponseEntity<Map<String,Object>> dalNotFound(HttpServletRequest request, DalNotFoundException ex) {
		return handleError(request,HttpStatus.NOT_FOUND, ex);
	}
	
	// Dal Runtime Exception
	@ExceptionHandler(DalRuntimeException.class)
	public ResponseEntity<Map<String,Object>> dalRT(HttpServletRequest request, DalRuntimeException ex) {
		return handleError(request,HttpStatus.INTERNAL_SERVER_ERROR, ex);
	}
	
	@ExceptionHandler(DataNotFoundException.class)
	public ResponseEntity<Map<String,Object>> dataNotFound(HttpServletRequest request, DataNotFoundException ex) {
		return handleError(request,HttpStatus.NOT_FOUND, ex);
	}
	
	@ExceptionHandler({HttpRequestMethodNotSupportedException.class, HttpMediaTypeException.class})
	public ResponseEntity<Map<String, Object>> badRequest(HttpServletRequest request, ServletException ex) {
	    return handleError(request, HttpStatus.BAD_REQUEST, ex);
	  }

}
