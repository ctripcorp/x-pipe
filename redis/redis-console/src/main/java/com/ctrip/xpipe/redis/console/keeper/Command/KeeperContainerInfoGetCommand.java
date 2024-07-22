package com.ctrip.xpipe.redis.console.keeper.command;

import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;

import java.util.List;

public class KeeperContainerInfoGetCommand extends AbstractGetAllDcCommand<List<KeeperContainerUsedInfoModel>> {

    public KeeperContainerInfoGetCommand(RestOperations restTemplate) {
        super(null, restTemplate);
    }

    @Override
    public String getName() {
        return "getKeeperContainerInfo";
    }

    @Override
    protected void doExecute() throws Throwable {
        try {
            ResponseEntity<List<KeeperContainerUsedInfoModel>> result =
                    restTemplate.exchange(domain + "/api/keepercontainer/info/all", HttpMethod.GET, null,
                            new ParameterizedTypeReference<List<KeeperContainerUsedInfoModel>>() {});
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
    public AbstractGetAllDcCommand<List<KeeperContainerUsedInfoModel>> clone() {
        return new KeeperContainerInfoGetCommand(restTemplate);
    }
}
