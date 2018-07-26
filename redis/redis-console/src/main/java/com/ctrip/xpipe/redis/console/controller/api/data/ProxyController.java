package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jul 26, 2018
 */

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class ProxyController {

    private JsonCodec pretty = new JsonCodec(true);

    @Autowired
    private ProxyService service;

    @RequestMapping(value = "/proxies/all", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public String getAllProxies() {
        try {
            List<ProxyModel> proxies = service.getAllProxies();
            return pretty.encode(proxies);
        } catch (Exception e) {
            return pretty.encode(RetMessage.createFailMessage(e.getMessage()));
        }
    }

    @RequestMapping(value = "/proxies/active", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public String getActiveProxies() {
        try {
            List<ProxyModel> proxies = service.getActiveProxies();
            return pretty.encode(proxies);
        } catch (Exception e) {
            return pretty.encode(RetMessage.createFailMessage(e.getMessage()));
        }
    }

    @RequestMapping(value = "/proxy", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage updateProxy(ProxyModel model) {
        try {
            service.updateProxy(model);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/proxy", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage addProxy(ProxyModel model) {
        try {
            service.addProxy(model);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/proxy", method = RequestMethod.DELETE, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage deleteProxy(ProxyModel model) {
        try {
            service.deleteProxy(model.getId());
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }
}
