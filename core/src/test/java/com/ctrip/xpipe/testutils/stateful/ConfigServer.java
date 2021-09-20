package com.ctrip.xpipe.testutils.stateful;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Slight
 * <p>
 * Sep 17, 2021 10:55 PM
 */
@SpringBootApplication
@ComponentScan("com.ctrip.xpipe.testutils.stateful.config")
@RestController
public class ConfigServer {

    @Autowired
    public StateHolder stateHolder;

    @RequestMapping("/state")
    @ResponseBody
    public String state() {
        return stateHolder.state();
    }
}
