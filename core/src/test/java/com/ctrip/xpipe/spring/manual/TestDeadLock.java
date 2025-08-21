package com.ctrip.xpipe.spring.manual;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.codec.Person;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.testutils.SpringApplicationStarter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestOperations;
import org.springframework.web.context.request.async.DeferredResult;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 * <p>
 * Jul 24, 2017
 */
public class TestDeadLock extends AbstractTest {

    private RestOperations restOperations;
    private SpringApplicationStarter springApplicationStarter;
    private int port = 8080;
    private int retryTimes = 2;
    private static int countInvoke = 0;

    @Before
    public void beforeRestTemplateFactoryTest() throws Exception {

        restOperations = RestTemplateFactory.createCommonsHttpRestTemplate(300
                , 1000
                , 1000
                , 100000
                , retryTimes);

        port = randomPort();
        springApplicationStarter = new SpringApplicationStarter(TestServer.class, port);
        springApplicationStarter.start();
    }

    @Test
    public void testConcurrentCall() throws InterruptedException {

        int concurrent = 1000;
        CountDownLatch latch = new CountDownLatch(concurrent);
        long start = System.nanoTime();
        for (int i = 0; i < concurrent; i++) {

            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Person person = restOperations.getForObject("http://localhost:" + port + "/person", Person.class);
                        logger.info("{}", person);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        latch.await();
        logger.info("[testConcurrentCall] finished: {}", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    }


//    @SpringBootApplication
//    @RestController
    public static class TestServer {

        private Logger logger = LoggerFactory.getLogger(getClass());
        private AtomicInteger concurrentCount = new AtomicInteger(0);

        private ExecutorService executors = Executors.newCachedThreadPool();

        @RequestMapping("/person")
        @ResponseBody
        public DeferredResult<String> person() {
            DeferredResult<String> result = new DeferredResult<String>();
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        int concurrent = concurrentCount.incrementAndGet();
                        logger.info("[person][begin] concurrent:{}", concurrent);
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        //ignore
                    } finally {
                        concurrentCount.decrementAndGet();
                    }

                    logger.info("[person][ end ]");
                    result.setResult("{\"sex\":\"FEMALE\",\"age\":1010, \"other\":11}");
                }
            });
            return result;

        }

        @RequestMapping("/502")
        @ResponseBody
        public void person502(HttpServletRequest request, HttpServletResponse response) {
            countInvoke++;
            response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
        }

        @RequestMapping("/505")
        @ResponseBody
        public void person505(HttpServletRequest request, HttpServletResponse response) {
            countInvoke++;
            response.setStatus(HttpServletResponse.SC_HTTP_VERSION_NOT_SUPPORTED);
        }

        @RequestMapping("/404")
        @ResponseBody
        public void person404(HttpServletRequest request, HttpServletResponse response) {
            countInvoke++;
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        }

    }

    @After
    public void afterRestTemplateFactoryTest() throws Exception {
        springApplicationStarter.stop();
    }
}
