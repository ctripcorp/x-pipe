package com.ctrip.xpipe.redis.console.spring;

import org.springframework.boot.web.server.ErrorPage;
import org.springframework.boot.web.server.ErrorPageRegistrar;
import org.springframework.boot.web.server.ErrorPageRegistry;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;

@Configuration
public class CustomizedErrorPageRegistrar implements ErrorPageRegistrar {

	@Override
	public void registerErrorPages(ErrorPageRegistry registry) {
		ErrorPage[] errorPages = new ErrorPage[1];
		errorPages[0] = new ErrorPage(HttpStatus.UNAUTHORIZED, "/401.html");
		registry.addErrorPages(errorPages);
	}

}
