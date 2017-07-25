package com.ctrip.xpipe.spring;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import org.junit.Test;
import org.springframework.web.client.RestOperations;

import java.io.IOException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 24, 2017
 */
public class RestTemplateFactoryTest extends AbstractTest {

    private String url = "http://localhost:8080/";


    @Test
    public void testConcurrent() throws IOException {

        int concurrentCount = 5;

        RestOperations commonsHttpRestTemplate = RestTemplateFactory.createCommonsHttpRestTemplate(2, 1000, 1000, 5000);

        for (int i = 0; i < concurrentCount; i++) {
            int finalI = i;
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {

                    logger.info("[doRun][begin]{}", finalI);
                    String response = commonsHttpRestTemplate.getForObject(url + "apple", String.class);
                    logger.info("testConcurrent:{}", response);
                    logger.info("[doRun][end]{}", finalI);
                }
            });
        }

        waitForAnyKey();
    }
}
