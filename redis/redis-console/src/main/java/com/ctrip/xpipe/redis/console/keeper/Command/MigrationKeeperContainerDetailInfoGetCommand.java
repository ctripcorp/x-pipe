package com.ctrip.xpipe.redis.console.keeper.Command;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;

import java.util.List;

public class MigrationKeeperContainerDetailInfoGetCommand extends AbstractGetAllDcCommand<List<MigrationKeeperContainerDetailModel>> {

    public MigrationKeeperContainerDetailInfoGetCommand(RestOperations restTemplate) {
        super(null, restTemplate);
    }

    @Override
    public String getName() {
        return "getMigrationKeeperContainerDetailInfo";
    }

    @Override
    protected void doExecute() throws Throwable {
        try {
            ResponseEntity<List<MigrationKeeperContainerDetailModel>> result =
                    restTemplate.exchange(domain + "/api/keepercontainer/overload/info/all", HttpMethod.GET, null,
                            new ParameterizedTypeReference<List<MigrationKeeperContainerDetailModel>>() {});
            future().setSuccess(result.getBody());
        } catch (Throwable th) {
            getLogger().error("get migration keeper container detail:{} fail", domain, th);
            future().setFailure(th);
        }
    }

    @Override
    protected void doReset() {

    }

    @Override
    public AbstractGetAllDcCommand<List<MigrationKeeperContainerDetailModel>> clone() {
        return new MigrationKeeperContainerDetailInfoGetCommand(restTemplate);
    }
}
