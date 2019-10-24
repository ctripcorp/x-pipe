package com.ctrip.xpipe.redis.meta.server.spring;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Health {

    @RequestMapping("/health")
    public boolean checkHealthStatus() {
        return true;
    }
}
