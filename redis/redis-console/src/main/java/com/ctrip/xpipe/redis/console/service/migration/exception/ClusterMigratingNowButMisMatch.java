package com.ctrip.xpipe.redis.console.service.migration.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 29, 2017
 */
public class ClusterMigratingNowButMisMatch extends RedisConsoleException {

    private String clusterName;
    private String fromIdc, toIdc;
    private long eventId;

    private String requestFromIdc;
    private String requestToIdc;


    public ClusterMigratingNowButMisMatch(String clusterName, String fromIdc, String toIdc,
                                          long eventId,
                                          String requestFromIdc, String requestToIdc) {
        super(String.format("cluster:%s, from:%s, to:%s, id:%s, requestFrom:%s, requestTo:%s",
                clusterName, fromIdc, toIdc, eventId, requestFromIdc, requestToIdc));
        this.clusterName = clusterName;
        this.fromIdc = fromIdc;
        this.toIdc = toIdc;
        this.eventId = eventId;
        this.requestFromIdc = requestFromIdc;
        this.requestToIdc = requestToIdc;
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
