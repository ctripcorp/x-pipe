package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.model.RouteModel;
import com.ctrip.xpipe.redis.console.service.RouteService;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jul 26, 2018
 */
@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class RouteController {

    private JsonCodec pretty = new JsonCodec(true, true);

    private static final Logger logger = LoggerFactory.getLogger(RouteController.class);

    @Autowired
    private RouteService service;

    @RequestMapping(value = "/routes/all", method = RequestMethod.GET)
    public String getAllRoutes() {
        logger.info("[getAllRoutes][begin]");
        try {
            List<RouteModel> proxies = service.getAllRoutes();
            return pretty.encode(proxies);
        } catch (Exception e) {
            logger.error("[getAllRoutes]", e);
            return pretty.encode(RetMessage.createFailMessage(e.getMessage()));
        }
    }

    @RequestMapping(value = "/routes/active", method = RequestMethod.GET)
    public String getActiveRoutes() {
        try {
            List<RouteModel> proxies = service.getActiveRoutes();
            return pretty.encode(proxies);
        } catch (Exception e) {
            logger.error("[getActiveRoutes]", e);
            return pretty.encode(RetMessage.createFailMessage(e.getMessage()));
        }
    }

    @RequestMapping(value = "/route", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage updateRoute(@RequestBody RouteModel model) {
        logger.info("[updateRoute] updated one: {}", model);
        try {
            service.updateRoute(model);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[updateRoute]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/route", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage addRoute(@RequestBody RouteModel model) {
        logger.info("[addRoute] add one: {}", model);
        try {
            service.addRoute(model);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[addRoute]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/route", method = RequestMethod.DELETE, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage deleteRoute(@RequestBody RouteModel model) {
        logger.info("[deleteRoute] delete one: {}", model);
        try {
            service.deleteRoute(model.getId());
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[deleteRoute]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/exist/route/active/{activeDc}/backup/{backupDc}", method = RequestMethod.GET)
    public RetMessage existRoutes(@PathVariable String activeDc, @PathVariable String backupDc) {
        logger.info("[existRoutes] {}, {}", activeDc, backupDc);
        if(StringUtil.trimEquals(activeDc, backupDc, true)) {
            return RetMessage.createFailMessage("false");
        }
        if(service.existsRouteBetweenDc(activeDc, backupDc)) {
            return RetMessage.createSuccessMessage();
        }
        return RetMessage.createFailMessage("false");
    }
}
