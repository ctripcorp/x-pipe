package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.consoleportal.DcListDcModel;
import com.ctrip.xpipe.redis.console.service.meta.impl.AdvancedDcMetaService;
import com.ctrip.xpipe.redis.core.entity.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * @author tt.tu
 * Oct 8, 2018
 */
public class DcServiceImplTest extends AbstractConsoleIntegrationTest {

    @InjectMocks
    private DcServiceImpl dcService = new DcServiceImpl();

    @Mock
    private AdvancedDcMetaService dcMetaService;
    private XpipeMeta xpipeMeta = new XpipeMeta();


    @Before
    public void beforeDcServiceImplTest() {
        MockitoAnnotations.initMocks(this);
        xpipeMeta = getXpipeMeta();
    }

    private List<DcTbl> toBuildTbl() {
        List<DcTbl> result = new LinkedList<>();

        DcTbl tbl1 = new DcTbl();
        tbl1.setDcName("jq").setId(1);

        DcTbl tbl2 = new DcTbl();
        tbl2.setDcName("oy").setId(2);

        DcTbl tbl3 = new DcTbl();
        tbl3.setDcName("fra").setId(3);

        result.add(tbl1);
        result.add(tbl2);
        result.add(tbl3);

        return result;
    }

    @Test
    public void testFindAllDcsRichinfo() throws Exception {
        Map<String, DcMeta> dcMetaMap = new HashMap<>();
        dcMetaMap.put("jq".toUpperCase(), xpipeMeta.findDc("jq"));
        dcMetaMap.put("oy".toUpperCase(), xpipeMeta.findDc("oy"));
        dcMetaMap.put("fra".toUpperCase(), xpipeMeta.findDc("fra"));
        when(dcMetaService.getAllDcMetas()).thenReturn(dcMetaMap);
        dcService = spy(dcService);
        Mockito.doReturn(toBuildTbl()).when(dcService).findAllDcs();
        List<DcListDcModel> result = dcService.findAllDcsRichInfo(false);
        Assert.assertEquals(3, result.size());
        result.forEach(dcListDcModel -> {
            if (Objects.equals(dcListDcModel.getDcName(), "jq")) {
                Assert.assertEquals(6, dcListDcModel.getClusterTypes().size());
                dcListDcModel.getClusterTypes().forEach(model -> {
                    if (model.getClusterType().equals(ClusterType.ONE_WAY.name())) {
                        Assert.assertEquals(4, model.getRedisCount());
                        Assert.assertEquals(2, model.getClusterCount());
                        Assert.assertEquals(2, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.SINGLE_DC.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(1, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.LOCAL_DC.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(0, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.CROSS_DC.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(0, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.HETERO.name())) {
                        Assert.assertEquals(4, model.getRedisCount());
                        Assert.assertEquals(2, model.getClusterCount());
                        Assert.assertEquals(2, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().isEmpty()) {
                        Assert.assertEquals(14, model.getRedisCount());
                        Assert.assertEquals(7, model.getClusterCount());
                        Assert.assertEquals(5, model.getClusterInActiveDcCount());
                    } else {
                        Assert.fail("no cluster type matched");
                    }
                });

            } else if (Objects.equals(dcListDcModel.getDcName(), "oy")) {
                Assert.assertEquals(5, dcListDcModel.getClusterTypes().size());
                dcListDcModel.getClusterTypes().forEach(model -> {
                    if (model.getClusterType().equals(ClusterType.ONE_WAY.name())) {
                        Assert.assertEquals(4, model.getRedisCount());
                        Assert.assertEquals(2, model.getClusterCount());
                        Assert.assertEquals(0, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.LOCAL_DC.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(0, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.CROSS_DC.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(0, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.HETERO.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(1, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().isEmpty()) {
                        Assert.assertEquals(10, model.getRedisCount());
                        Assert.assertEquals(5, model.getClusterCount());
                        Assert.assertEquals(1, model.getClusterInActiveDcCount());
                    } else {
                        Assert.fail("no cluster type matched");
                    }
                });
            } else if (Objects.equals(dcListDcModel.getDcName(), "fra")) {
                Assert.assertEquals(2, dcListDcModel.getClusterTypes().size());
                dcListDcModel.getClusterTypes().forEach(model -> {
                    if (model.getClusterType().equals(ClusterType.HETERO.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(1, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().isEmpty()) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(1, model.getClusterInActiveDcCount());
                    } else {
                        Assert.fail("no cluster type matched");
                    }
                });
            }
        });
    }

    @Test
    public void testFindAllDcsRichinfoForHetero() throws Exception {
        Map<String, DcMeta> dcMetaMap = new HashMap<>();
        dcMetaMap.put("jq".toUpperCase(), xpipeMeta.findDc("jq"));
        dcMetaMap.put("oy".toUpperCase(), xpipeMeta.findDc("oy"));
        dcMetaMap.put("fra".toUpperCase(), xpipeMeta.findDc("fra"));
        when(dcMetaService.getAllDcMetas()).thenReturn(dcMetaMap);
        dcService = spy(dcService);
        Mockito.doReturn(toBuildTbl()).when(dcService).findAllDcs();
        List<DcListDcModel> result = dcService.findAllDcsRichInfo(true);
        Assert.assertEquals(3, result.size());
        result.forEach(dcListDcModel -> {
            if (Objects.equals(dcListDcModel.getDcName(), "jq")) {
                Assert.assertEquals(6, dcListDcModel.getClusterTypes().size());
                dcListDcModel.getClusterTypes().forEach(model -> {
                    if (model.getClusterType().equals(ClusterType.ONE_WAY.name())) {
                        Assert.assertEquals(8, model.getRedisCount());
                        Assert.assertEquals(4, model.getClusterCount());
                        Assert.assertEquals(4, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.SINGLE_DC.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(1, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.LOCAL_DC.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(0, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.CROSS_DC.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(0, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.HETERO.name())) {
                        Assert.assertEquals(4, model.getRedisCount());
                        Assert.assertEquals(2, model.getClusterCount());
                        Assert.assertEquals(2, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().isEmpty()) {
                        Assert.assertEquals(14, model.getRedisCount());
                        Assert.assertEquals(7, model.getClusterCount());
                        Assert.assertEquals(5, model.getClusterInActiveDcCount());
                    } else {
                        Assert.fail("no cluster type matched");
                    }
                });

            } else if (Objects.equals(dcListDcModel.getDcName(), "oy")) {
                Assert.assertEquals(6, dcListDcModel.getClusterTypes().size());
                dcListDcModel.getClusterTypes().forEach(model -> {
                    if (model.getClusterType().equals(ClusterType.ONE_WAY.name())) {
                        Assert.assertEquals(4, model.getRedisCount());
                        Assert.assertEquals(2, model.getClusterCount());
                        Assert.assertEquals(0, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.SINGLE_DC.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(1, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.LOCAL_DC.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(0, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.CROSS_DC.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(0, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.HETERO.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(1, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().isEmpty()) {
                        Assert.assertEquals(10, model.getRedisCount());
                        Assert.assertEquals(5, model.getClusterCount());
                        Assert.assertEquals(1, model.getClusterInActiveDcCount());
                    } else {
                        Assert.fail("no cluster type matched");
                    }
                });
            } else if (Objects.equals(dcListDcModel.getDcName(), "fra")) {
                Assert.assertEquals(3, dcListDcModel.getClusterTypes().size());
                dcListDcModel.getClusterTypes().forEach(model -> {
                    if (model.getClusterType().equals(ClusterType.SINGLE_DC.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(1, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.HETERO.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(1, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().isEmpty()) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(1, model.getClusterInActiveDcCount());
                    } else {
                        Assert.fail("no cluster type matched");
                    }
                });
            }
        });
    }

    protected String getXpipeMetaConfigFile() {
        return "dc-stats-test.xml";
    }
}
