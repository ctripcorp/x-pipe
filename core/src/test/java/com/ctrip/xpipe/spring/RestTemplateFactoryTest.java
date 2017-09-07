package com.ctrip.xpipe.spring;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.codec.Person;
import com.ctrip.xpipe.testutils.SpringApplicationStarter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
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

    @Before
    public void beforeRestTemplateFactoryTest() throws Exception {

        restOperations = RestTemplateFactory.createCommonsHttpRestTemplate(2, 1000, 1000, 5000);

        port = randomPort();
        springApplicationStarter = new SpringApplicationStarter(SpringBootServer.class, port);
        springApplicationStarter.start();
    }


    @After
    public void afterRestTemplateFactoryTest() throws Exception {
        springApplicationStarter.stop();
    }

    @Test
    public void testJson() {
        Person person = restOperations.getForObject("http://localhost:" + port + "/person", Person.class);
        logger.info("{}", person);
    }
}
