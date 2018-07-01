package com.ctrip.xpipe.redis.proxy.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author chen.zhu
 * <p>
 * Jun 07, 2018
 */
@RestController
public class Health {
    @RequestMapping("/health")
    public boolean health() {
        return true;
    }
}
