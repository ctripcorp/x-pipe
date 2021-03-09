package com.ctrip.xpipe.redis.checker.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author lishanglin
 * date 2021/3/8
 */
@RestController
public class CheckerController {

    @GetMapping(value = "/ping")
    public String test() {
        return "pong";
    }

}
