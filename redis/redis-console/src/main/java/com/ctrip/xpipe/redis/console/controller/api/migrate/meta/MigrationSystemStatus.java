package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lishanglin
 * date 2022/4/3
 */
public class MigrationSystemStatus {

    public String status;

    public String message;

    public long checkStartTime;

    public Map<String, Long> checkUseTimeMill;

    public MigrationSystemStatus(String status, String message, long checkStartTime) {
        this.status = status;
        this.message = message;
        this.checkStartTime = checkStartTime;
        this.checkUseTimeMill = new HashMap<>();
    }

}
