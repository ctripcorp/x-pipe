package com.ctrip.xpipe.spring;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.codec.Person;
import com.ctrip.xpipe.testutils.SpringApplicationStarter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestOperations;


/**
 * @author wenchao.meng
 *         <p>
 *         Jul 24, 2017
 */
public class RestTemplateFactoryTest extends AbstractTest {

    private RestOperations restOperations;
    private SpringApplicationStarter springApplicationStarter;
    private int port = 8080;
    private int retryTimes = 2;
    private static int countInvoke = 0;

    @Before
    public void beforeRestTemplateFactoryTest() throws Exception {

        restOperations = RestTemplateFactory.createCommonsHttpRestTemplate(2
                , 1000
                , 1000
                , 5000
                , retryTimes);

        port = randomPort();
        springApplicationStarter = new SpringApplicationStarter(TestServer.class, port);
        springApplicationStarter.start();
    }

    @Test
    public void testRetry502(){

        int beginCount =  countInvoke;
        try{
            Person person = restOperations.getForObject("http://localhost:" + port + "/502", Person.class);
        }catch (Exception e){
            logger.error("[testRetry502]", e);
        }
        int endCount =  countInvoke;
        Assert.assertEquals(retryTimes + 1, endCount - beginCount);
    }

    @Test
    public void testNoRetry505(){

        int beginCount =  countInvoke;
        try{
            Person person = restOperations.getForObject("http://localhost:" + port + "/505", Person.class);
        }catch (Exception e){
            logger.error("[testRetry502]", e);
        }
        int endCount =  countInvoke;
        Assert.assertEquals(1, endCount - beginCount);
    }

    @Test
    public void testNotry404(){

        int beginCount =  countInvoke;
        try{
            Person person = restOperations.getForObject("http://localhost:" + port + "/404", Person.class);
        }catch (Exception e){
            logger.error("[testRetry404]", e);
        }
        int endCount =  countInvoke;
        Assert.assertEquals(1, endCount - beginCount);

    }


    @Test
    public void testJson() {

        Person person = restOperations.getForObject("http://localhost:" + port + "/person", Person.class);
        logger.info("{}", person);

    }


    @SpringBootApplication
    @RestController
    public static class TestServer {

        @RequestMapping("/person")
        @ResponseBody
        public String person() {
            return "{\"sex\":\"FEMALE\",\"age\":1010, \"other\":11}";
        }

        @RequestMapping("/502")
        @ResponseBody
        public void person502(HttpServletRequest request, HttpServletResponse response){
            countInvoke++;
            response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
        }

        @RequestMapping("/505")
        @ResponseBody
        public void person505(HttpServletRequest request, HttpServletResponse response){
            countInvoke++;
            response.setStatus(HttpServletResponse.SC_HTTP_VERSION_NOT_SUPPORTED);
        }

        @RequestMapping("/404")
        @ResponseBody
        public void person404(HttpServletRequest request, HttpServletResponse response){
            countInvoke++;
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        }

    }

    @After
    public void afterRestTemplateFactoryTest() throws Exception {
        springApplicationStarter.stop();
    }
}
