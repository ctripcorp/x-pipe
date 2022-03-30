package com.ctrip.xpipe.redis.console.spring;

import org.springframework.boot.autoconfigure.web.servlet.WebMvcRegistrations;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;

/**
 * @author Slight
 * <p>
 * Mar 18, 2022 7:51 PM
 */
public interface XPipeMvcRegistrations extends WebMvcRegistrations {

    @Override
    default RequestMappingHandlerAdapter getRequestMappingHandlerAdapter() {
        return new XPipeHandlerAdapter();
    }
}
