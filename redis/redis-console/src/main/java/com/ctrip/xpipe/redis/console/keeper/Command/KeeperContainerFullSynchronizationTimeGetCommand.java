package com.ctrip.xpipe.redis.console.keeper.Command;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;

import java.util.List;

public class KeeperContainerFullSynchronizationTimeGetCommand extends AbstractGetAllDcCommand<List<Integer>> {

    public KeeperContainerFullSynchronizationTimeGetCommand(RestOperations restTemplate) {
        super(null, restTemplate);
    }

    @Override
    public String getName() {
        return "getKeeperContainerFullSynchronizationTime";
    }

    @Override
    protected void doExecute() throws Throwable {
        try {
            ResponseEntity<List<Integer>> result =
                    restTemplate.exchange(domain + "/api/keepercontainer/full/synchronization/time", HttpMethod.GET, null,
                            new ParameterizedTypeReference<List<Integer>>() {});
            future().setSuccess(result.getBody());
        } catch (Throwable th) {
            getLogger().error("get keeper container info:{} fail", domain, th);
            future().setFailure(th);
        }
    }

    @Override
    protected void doReset() {

    }

    @Override
    public AbstractGetAllDcCommand<List<Integer>> clone() {
        return new KeeperContainerFullSynchronizationTimeGetCommand(restTemplate);
    }
}
