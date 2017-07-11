package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 22, 2017
 */
public class CheckStatusResponse extends AbstractResponseMeta{

    private List<CheckStatusClusterResponse>  results = new LinkedList<>();

    public List<CheckStatusClusterResponse> getResults() {
        return results;
    }

    public synchronized void addResult(CheckStatusClusterResponse response){

        results.forEach((result) -> {

            if(result.getClusterName().equals(response.getClusterName())){
                throw new IllegalArgumentException(
                        String.format("cluster already exist, old:%s, current:%s", result, response)
                );
            }
        });
        results.add(response);
    }

}
