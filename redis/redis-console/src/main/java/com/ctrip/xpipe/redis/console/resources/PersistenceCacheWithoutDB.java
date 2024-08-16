package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;

public class PersistenceCacheWithoutDB extends CheckerPersistenceCache {

    private ConsoleConfig consoleConfig;

    public PersistenceCacheWithoutDB(ConsoleConfig config, CheckerConsoleService service) {
        super(config, service);
        this.service = service;
        this.consoleConfig = config;
    }

    protected String getConsoleAddress() {
        return consoleConfig.getConsoleDomain();
    }

}
