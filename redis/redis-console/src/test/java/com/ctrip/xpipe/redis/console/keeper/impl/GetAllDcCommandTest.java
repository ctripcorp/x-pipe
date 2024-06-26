package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.keeper.Command.AbstractGetAllDcCommand;
import com.ctrip.xpipe.redis.console.keeper.Command.KeeperContainerInfoGetCommand;
import com.ctrip.xpipe.redis.console.keeper.Command.MigrationKeeperContainerDetailInfoGetCommand;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.anyString;

@RunWith(org.mockito.junit.MockitoJUnitRunner.class)
public class GetAllDcCommandTest {

    @Mock
    private RestTemplate restTemplate;

    private static final String domain = "domain";

    @Test
    public void keeperContainerInfoGetCommandTest() throws ExecutionException, InterruptedException {
        ResponseEntity<List<KeeperContainerUsedInfoModel>> response = new ResponseEntity<>(new ArrayList<>(), HttpStatus.OK);
        KeeperContainerInfoGetCommand command = new KeeperContainerInfoGetCommand(restTemplate);
        Mockito.when(restTemplate.exchange(domain + "/api/keepercontainer/info/all", HttpMethod.GET, null,
                new ParameterizedTypeReference<List<KeeperContainerUsedInfoModel>>() {})).thenReturn(response);
        AbstractGetAllDcCommand<List<KeeperContainerUsedInfoModel>> clone = command.clone();
        clone.setDomain("domain");
        List<KeeperContainerUsedInfoModel> list = clone.execute().get();
        Assert.assertNotNull(list);
    }

    @Test
    public void migrationKeeperContainerDetailInfoGetCommandTest() throws ExecutionException, InterruptedException {
        ResponseEntity<List<MigrationKeeperContainerDetailModel>> response = new ResponseEntity<>(new ArrayList<>(), HttpStatus.OK);
        MigrationKeeperContainerDetailInfoGetCommand command = new MigrationKeeperContainerDetailInfoGetCommand(restTemplate);
        Mockito.when(restTemplate.exchange(domain + "/api/keepercontainer/overload/info/all", HttpMethod.GET, null,
                new ParameterizedTypeReference<List<MigrationKeeperContainerDetailModel>>() {})).thenReturn(response);
        AbstractGetAllDcCommand<List<MigrationKeeperContainerDetailModel>> clone = command.clone();
        clone.setDomain("domain");
        List<MigrationKeeperContainerDetailModel> list = clone.execute().get();
        Assert.assertNotNull(list);
    }

}
