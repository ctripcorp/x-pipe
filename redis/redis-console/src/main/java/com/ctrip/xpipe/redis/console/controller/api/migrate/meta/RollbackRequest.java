package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 22, 2017
 */
public class RollbackRequest extends AbstractTicketIdRequest{

    private List<String> clusters;

    public List<String> getClusters() {
        return clusters;
    }

    public void setClusters(List<String> clusters) {
        this.clusters = clusters;
    }
}
