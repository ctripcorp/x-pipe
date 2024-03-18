package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

import java.util.Set;

/**
 * @author lishanglin
 * date 2024/3/18
 */
public class MigrationHistoryReq {

    public long from;

    public long to;

    public Set<String> clusters;

}
