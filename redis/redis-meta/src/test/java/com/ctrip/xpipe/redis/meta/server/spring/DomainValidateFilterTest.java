package com.ctrip.xpipe.redis.meta.server.spring;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerContextTest;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.servlet.HandlerExecutionChain;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;


import static com.ctrip.xpipe.spring.DomainValidateFilter.BAD_REQUEST_CODE;
import static com.ctrip.xpipe.spring.DomainValidateFilter.BAD_REQUEST_MESSAGE_TEMPLATE;
import static com.ctrip.xpipe.spring.DomainValidateFilter.STOP_CHECK_URI;

@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@SpringBootTest(classes = DomainValidateFilterTest.MetaServerTestConfig.class)
public class DomainValidateFilterTest extends AbstractMetaServerContextTest {

    private Logger logger = LoggerFactory.getLogger(DomainValidateFilterTest.class);

    @Autowired
    private RequestMappingHandlerAdapter handlerAdapter;

    @Autowired
    private RequestMappingHandlerMapping handlerMapping;

    @Autowired
    private MetaServerConfig config;

    @Test
    public void testInterceptor() throws Exception{

        MockHttpServletRequest request = getMockedRequest("localhost:9747");
        logger.info("[dc name] {}", FoundationService.DEFAULT.getDataCenter());
        Assert.assertEquals("http://localhost:9747", config.getDcInofs()
                .get(FoundationService.DEFAULT.getDataCenter()).getMetaServerAddress());

        MockHttpServletResponse response = execute(request);

        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void testInterceptorWithErrorCode() throws Exception{

        MockHttpServletRequest request = getMockedRequest("error.domain");

        MockHttpServletResponse response = execute(request);
        Assert.assertEquals(BAD_REQUEST_CODE, response.getStatus());
        Assert.assertEquals(String.format(BAD_REQUEST_MESSAGE_TEMPLATE, "error.domain", "localhost"),
                response.getErrorMessage());
    }

    @Test
    public void testInterceptorWithIp() throws Exception{

        String notEventExistIp = "10.0.0.1:8080";
        MockHttpServletRequest request = getMockedRequest(notEventExistIp);

        MockHttpServletResponse response = execute(request);
        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void testShutDownValidationProcess() throws Exception {
        MockHttpServletRequest request = getMockedRequest("error.domain");

        MockHttpServletResponse response = execute(request);
        Assert.assertEquals(BAD_REQUEST_CODE, response.getStatus());

        request = getMockedRequest("127.0.0.1:8080");
        request.setRequestURI(STOP_CHECK_URI);
        execute(request);

        request = getMockedRequest("error.domain");
        response = execute(request);
        Assert.assertEquals(200, response.getStatus());
    }

    private MockHttpServletRequest getMockedRequest(String host) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRequestURI("/api/meta/getactivekeeper/cluster1/shard1");
        request.setMethod("GET");
        request.addHeader("host", host);

        return request;

    }

    private MockHttpServletResponse execute(MockHttpServletRequest request) throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();

        HandlerExecutionChain handlerExecutionChain = handlerMapping.getHandler(request);

        Assert.assertNotNull(handlerExecutionChain);

        HandlerInterceptor[] interceptors = handlerExecutionChain.getInterceptors();



        for(HandlerInterceptor interceptor : interceptors){
            interceptor.preHandle(request, response, handlerExecutionChain.getHandler());
        }
        return response;
    }

    @SpringBootApplication
    public static class MetaServerTestConfig{

    }
}
