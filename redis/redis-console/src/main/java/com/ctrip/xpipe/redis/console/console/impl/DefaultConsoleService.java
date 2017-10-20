package com.ctrip.xpipe.redis.console.console.impl;

import com.ctrip.xpipe.redis.console.console.ConsoleService;
import com.ctrip.xpipe.redis.console.health.action.HEALTH_STATE;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import org.springframework.web.client.RestOperations;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 07, 2017
 */
public class DefaultConsoleService implements ConsoleService{

    private String address;
    private RestOperations restOperations;

    private final String healthStatusUrl;

    public DefaultConsoleService(String address){

        this.address = address;
        if(!this.address.startsWith("http://")){
            this.address = "http://" + this.address;
        }
        healthStatusUrl = String.format("%s/api/health/{ip}/{port}", this.address);
        this.restOperations = RestTemplateFactory.createCommonsHttpRestTemplate(3, 10);
    }

    @Override
    public HEALTH_STATE getInstanceStatus(String ip, int port) {
        return restOperations.getForObject(healthStatusUrl, HEALTH_STATE.class, ip, port);
    }

    @Override
    public String toString() {
        return String.format("%s", address);
    }
}
