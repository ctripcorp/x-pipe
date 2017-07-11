package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 22, 2017
 */
public class RollbackResponse extends AbstractResponseMeta{

    private List<RollbackClusterResponse> results = new LinkedList<>();

    public RollbackResponse(){

    }

    public synchronized void addResult(RollbackClusterResponse rollbackClusterResponse){

        results.forEach(rollbackResponse -> {
            if(rollbackResponse.getClusterName().equals(rollbackClusterResponse.getClusterName())){
                throw new IllegalArgumentException("cluster already exist:" + rollbackClusterResponse.getClusterName());
            }
        });

        results.add(rollbackClusterResponse);

    }

    public List<RollbackClusterResponse> getResults() {
        return results;
    }
}
