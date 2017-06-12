package com.ctrip.xpipe.service;

import com.ctrip.xpipe.AbstractServiceTest;
import com.google.common.util.concurrent.AbstractService;
import org.junit.After;
import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 12, 2017
 */
@SpringBootApplication
public class AppTest extends AbstractServiceTest{


    @Test
    public void startAppTest(){
        SpringApplication.run(AppTest.class);
    }


    @After
    public void afterAppTest() throws IOException {
        waitForAnyKeyToExit();
    }

}
