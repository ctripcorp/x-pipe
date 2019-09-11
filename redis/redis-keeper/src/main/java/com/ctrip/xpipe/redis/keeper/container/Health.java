package com.ctrip.xpipe.redis.keeper.container;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author tt.tu
 * Sep 11, 2019
 */

@RestController
public class Health {

    @RequestMapping("/health")
    public boolean checkHealthStatus() {
        return true;
    }
}
