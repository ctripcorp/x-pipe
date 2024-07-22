package com.ctrip.xpipe.redis.console.model;

import java.util.List;

/**
 * @author lishanglin
 * date 2024/7/21
 */
public class BiMigrationReq {

    public List<DcTbl> excludedDcs;

    public List<ClusterTbl> clusters;

}
