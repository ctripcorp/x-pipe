package com.ctrip.xpipe.spring;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.ModelAndView;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * @author wenchao.meng
 *
 * Aug 1, 2016
 */
public class ExceptionLoggerResolver implements HandlerExceptionResolver{
	
	protected Logger logger = LoggerFactory.getLogger(getClass()); 

	@Override
	public ModelAndView resolveException(HttpServletRequest request, HttpServletResponse response, Object handler,
			Exception ex) {
		logger.error(request.getRequestURI().toString(), ex);
		return null;
	}

}
