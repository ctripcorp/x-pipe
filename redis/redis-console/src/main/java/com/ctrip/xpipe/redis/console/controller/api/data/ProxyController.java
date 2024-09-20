package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.service.ProxyService;
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
public class ProxyController {

    private JsonCodec pretty = new JsonCodec(true, true);

    private static final Logger logger = LoggerFactory.getLogger(ProxyController.class);

    @Autowired
    private ProxyService service;

    @RequestMapping(value = "/proxies/all", method = RequestMethod.GET)
    public String getAllProxies() {
        try {
            List<ProxyModel> proxies = service.getAllProxies();
            return pretty.encode(proxies);
        } catch (Exception e) {
            logger.error("[getAllProxies]", e);
            return pretty.encode(RetMessage.createFailMessage(e.getMessage()));
        }
    }

    @RequestMapping(value = "/proxies/monitor_active", method = RequestMethod.GET)
    public String getMonitorActiveProxiesByDc(@RequestParam(value="dc") String dc) {
        return pretty.encode(service.getMonitorActiveProxiesByDc(dc));
    }



    @RequestMapping(value = "/proxies/active", method = RequestMethod.GET)
    public String getActiveProxies() {
        try {
            List<ProxyModel> proxies = service.getActiveProxies();
            return pretty.encode(proxies);
        } catch (Exception e) {
            logger.error("[getActiveProxies]", e);
            return pretty.encode(RetMessage.createFailMessage(e.getMessage()));
        }
    }

    @RequestMapping(value = "/proxy", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage updateProxy(@RequestBody ProxyModel model) {
        logger.info("[updateProxy] updated one: {}", model);
        try {
            service.updateProxy(model);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[updateProxy]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/proxy", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage addProxy(@RequestBody ProxyModel model) {
        logger.info("[addProxy] add one: {}", model);
        try {
            service.addProxy(model);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[addProxy]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/proxy", method = RequestMethod.DELETE, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage deleteProxy(@RequestBody ProxyModel model) {
        logger.info("[deleteProxy] delete one: {}", model);
        try {
            service.deleteProxy(model.getId());
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[deleteProxy]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }
}
