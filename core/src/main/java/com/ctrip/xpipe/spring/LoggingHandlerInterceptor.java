package com.ctrip.xpipe.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * @author wenchao.meng
 *
 * Aug 30, 2016
 */
public class LoggingHandlerInterceptor implements HandlerInterceptor{
	
	private Logger logger = LoggerFactory.getLogger(LoggingHandlerInterceptor.class);

	@Override
	public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
			throws Exception {
		
		if(log(request)){
			logger.info("[preHandle]{}", request.getRequestURI());
		}
		return true;
	}

	private boolean log(HttpServletRequest request) {
		String path = request.getRequestURL().toString();
		if(path.startsWith("/")){
			return true;
		}
		return false;
	}

	@Override
	public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler,
			ModelAndView modelAndView) throws Exception {

		if(log(request)){
			logger.info("[postHandle]{}, {}", request.getRequestURI(), modelAndView);
		}
	}

	@Override
	public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex)
			throws Exception {

		if(log(request)){
			if(ex != null){
				logger.error("[afterCompletion]" +  request.getRequestURI(), ex);
			}else{
				logger.info("[afterCompletion]{}", request.getRequestURI());				
			}
		}
	}

}
