package com.ctrip.xpipe.redis.console.console.impl;

import com.ctrip.xpipe.redis.console.console.ConsoleService;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.core.service.AbstractService;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 07, 2017
 */
public class DefaultConsoleService extends AbstractService implements ConsoleService{

    private String address;

    private final String healthStatusUrl;

    private final String pingStatusUrl;

    private final String dbPingStatsUrl;

    public DefaultConsoleService(String address){

        this.address = address;
        if(!this.address.startsWith("http://")){
            this.address = "http://" + this.address;
        }
        healthStatusUrl = String.format("%s/api/health/{ip}/{port}", this.address);
        pingStatusUrl = String.format("%s/api/ping/{ip}/{port}", this.address);
        dbPingStatsUrl = String.format("%s/api/db/affinity", this.address);
    }

    @Override
    public HEALTH_STATE getInstanceStatus(String ip, int port) {
        return restTemplate.getForObject(healthStatusUrl, HEALTH_STATE.class, ip, port);
    }

    @Override
    public Boolean getInstancePingStatus(String ip, int port) {
        return restTemplate.getForObject(pingStatusUrl, Boolean.class, ip, port);
    }

    @Override
    public Long getConsoleDatabaseAffinity() {
        return restTemplate.getForObject(dbPingStatsUrl, Long.class);
    }

    @Override
    public String toString() {
        return String.format("%s", address);
    }
}
