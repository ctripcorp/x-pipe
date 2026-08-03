package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.AzGroupCreateInfo;
import com.ctrip.xpipe.redis.console.model.AzGroupModel;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Phase E: AzGroup definition management API — create / delete / getAll.
 */
public class AzGroupUpdateControllerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private AzGroupUpdateController azGroupUpdateController;

    @Autowired
    private AzGroupCache azGroupCache;

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/hetero-cross-region-az-group-test.sql");
    }

    @Test
    public void testCreateCrossRegionAzGroupSuccess() {
        AzGroupCreateInfo createInfo = new AzGroupCreateInfo()
                .setName("CROSS_JQ_SGP")
                .setAzs(Arrays.asList("jq", "sgp"));

        RetMessage ret = azGroupUpdateController.createAzGroup(createInfo);
        Assert.assertEquals(RetMessage.SUCCESS_STATE, ret.getState());

        List<AzGroupModel> all = azGroupUpdateController.getAllAzGroups();
        AzGroupModel created = all.stream()
                .filter(g -> "CROSS_JQ_SGP".equals(g.getName()))
                .findFirst()
                .orElse(null);
        Assert.assertNotNull(created);
        Assert.assertEquals(new HashSet<>(Arrays.asList("jq", "sgp")), created.getAzs());

        AzGroupModel byAzs = azGroupCache.getAzGroupByAzs(Arrays.asList("jq", "sgp"));
        Assert.assertNotNull(byAzs);
        Assert.assertEquals("CROSS_JQ_SGP", byAzs.getName());
    }

    @Test
    public void testCreateRejectsDuplicateName() {
        AzGroupCreateInfo createInfo = new AzGroupCreateInfo()
                .setName("CROSS_SHA_FRA")
                .setAzs(Arrays.asList("jq", "sgp"));

        RetMessage ret = azGroupUpdateController.createAzGroup(createInfo);
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());
        Assert.assertTrue(ret.getMessage(), ret.getMessage().contains("already exists"));
    }

    @Test
    public void testCreateRejectsUnknownAz() {
        AzGroupCreateInfo createInfo = new AzGroupCreateInfo()
                .setName("CROSS_UNKNOWN")
                .setAzs(Arrays.asList("jq", "no-such-az"));

        RetMessage ret = azGroupUpdateController.createAzGroup(createInfo);
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());
        Assert.assertTrue(ret.getMessage(), ret.getMessage().contains("does not exist"));
    }

    @Test
    public void testCreateRejectsDuplicateAzsSet() {
        AzGroupCreateInfo createInfo = new AzGroupCreateInfo()
                .setName("DUP_AZS_SHA")
                .setAzs(Arrays.asList("jq", "oy"));

        RetMessage ret = azGroupUpdateController.createAzGroup(createInfo);
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());
        Assert.assertTrue(ret.getMessage(), ret.getMessage().contains("already used"));
    }

    @Test
    public void testDeleteSuccessWhenNoReference() {
        RetMessage createRet = azGroupUpdateController.createAzGroup(new AzGroupCreateInfo()
                .setName("TO_DELETE")
                .setAzs(Arrays.asList("oy", "sgp")));
        Assert.assertEquals(RetMessage.SUCCESS_STATE, createRet.getState());

        RetMessage deleteRet = azGroupUpdateController.deleteAzGroupByName("TO_DELETE");
        Assert.assertEquals(RetMessage.SUCCESS_STATE, deleteRet.getState());

        Assert.assertNull(azGroupCache.getAzGroupByAzs(Arrays.asList("oy", "sgp")));
        Assert.assertTrue(azGroupUpdateController.getAllAzGroups().stream()
                .noneMatch(g -> "TO_DELETE".equals(g.getName())));
    }

    @Test
    public void testDeleteRejectsWhenAzGroupClusterReferences() {
        RetMessage ret = azGroupUpdateController.deleteAzGroupByName("CROSS_SHA_FRA");
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());
        Assert.assertTrue(ret.getMessage(), ret.getMessage().contains("referenced"));

        Assert.assertNotNull(azGroupCache.getAzGroupByAzs(Arrays.asList("jq", "oy", "fra")));
    }

    @Test
    public void testDeleteRejectsWhenNotFound() {
        RetMessage ret = azGroupUpdateController.deleteAzGroupByName("NO_SUCH_GROUP");
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());
        Assert.assertTrue(ret.getMessage(), ret.getMessage().contains("not found"));
    }

    @Test
    public void testGetAllContainsFixtureGroups() {
        List<AzGroupModel> all = azGroupUpdateController.getAllAzGroups();
        Assert.assertFalse(all.isEmpty());
        Assert.assertTrue(all.stream().anyMatch(g -> "CROSS_SHA_FRA".equals(g.getName())));
        Assert.assertTrue(all.stream().anyMatch(g -> "LOCAL_SGP".equals(g.getName())));
        Assert.assertTrue(all.stream().anyMatch(g -> "LOCAL_SHA".equals(g.getName())));
    }

    @Test
    public void testCreateRejectsEmptyName() {
        AzGroupCreateInfo createInfo = new AzGroupCreateInfo()
                .setName("")
                .setAzs(Collections.singletonList("jq"));
        RetMessage ret = azGroupUpdateController.createAzGroup(createInfo);
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());
        Assert.assertTrue(ret.getMessage(), ret.getMessage().contains("name empty"));
    }
}
