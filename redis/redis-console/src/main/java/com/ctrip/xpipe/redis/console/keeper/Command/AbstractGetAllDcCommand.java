package com.ctrip.xpipe.redis.console.keeper.Command;

import com.ctrip.xpipe.command.AbstractCommand;
import org.springframework.web.client.RestOperations;

public abstract class AbstractGetAllDcCommand<T> extends AbstractCommand<T> {
    protected String domain;
    protected RestOperations restTemplate;

    protected AbstractGetAllDcCommand(String domain, RestOperations restTemplate) {
        this.domain = domain;
        this.restTemplate = restTemplate;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public abstract AbstractGetAllDcCommand<T> clone ();

}
