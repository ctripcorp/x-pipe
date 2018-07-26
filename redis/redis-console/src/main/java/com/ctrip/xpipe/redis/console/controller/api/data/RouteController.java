package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.service.RouteService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author chen.zhu
 * <p>
 * Jul 26, 2018
 */
@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class RouteController {

    private JsonCodec pretty = new JsonCodec(true);

    @Autowired
    private RouteService service;
}
