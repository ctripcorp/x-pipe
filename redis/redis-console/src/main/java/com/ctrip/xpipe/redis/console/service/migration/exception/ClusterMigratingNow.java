package com.ctrip.xpipe.redis.console.service.migration.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 29, 2017
 */
public class ClusterMigratingNow extends RedisConsoleException {

    private String clusterName;
    private String fromIdc, toIdc;
    private long eventId;


    public ClusterMigratingNow(String clusterName, String fromIdc, String toIdc, long eventId) {
        super(String.format("cluster:%s, from:%s, to:%s, id:%s", clusterName, fromIdc, toIdc, eventId));
        this.clusterName = clusterName;
        this.fromIdc = fromIdc;
        this.toIdc = toIdc;
        this.eventId = eventId;
    }

    public String getClusterName() {
        return clusterName;
    }

    public long getEventId() {
        return eventId;
    }

    public String getFromIdc() {
        return fromIdc;
    }

    public String getToIdc() {
        return toIdc;
    }
}
