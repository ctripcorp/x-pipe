package com.ctrip.xpipe.spring;

import com.ctrip.xpipe.codec.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 24, 2017
 */
@SpringBootApplication
@RestController
public class SpringBootServer {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @RequestMapping("/apple")
    @ResponseBody
    public String homeApple() throws InterruptedException {
        logger.info("[homeApple][begin]");
        TimeUnit.SECONDS.sleep(2);
        logger.info("[homeApple][end]");
        return "Apple!";
    }

    @RequestMapping("/person")
    @ResponseBody
    public String getPersonMoreElements() throws InterruptedException {
        return "{\"sex\":\"FEMALE\",\"age\":100, \"haha\":\"fail\"}";
    }




    public static void main(String []argc){

        new SpringApplication(SpringBootServer.class).run();

    }
}
