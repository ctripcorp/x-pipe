package com.ctrip.xpipe.service;

import org.junit.After;
import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 12, 2017
 */
@SpringBootApplication
@Controller
public class AppTest extends AbstractServiceTest{

    @RequestMapping("/api/apple")
    @ResponseBody
    String homeApple() {
        return "Apple!";
    }


    @Test
    public void startAppTest(){
        SpringApplication.run(AppTest.class);
    }


    @After
    public void afterAppTest() throws IOException {
        waitForAnyKeyToExit();
    }

}
