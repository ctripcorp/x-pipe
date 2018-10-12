package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.LinkedList;
import java.util.List;


public class ClusterServiceImplTest3 extends AbstractConsoleIntegrationTest{


    @Autowired
    private ClusterService clusterService;

    @Before
    public void beforeStart(){
        MockitoAnnotations.initMocks(this);

    }

    @Override
    public String prepareDatas(){
        try{
            return prepareDatasFromFile("src/test/resources/cluster-service-impl-test.sql");
        }catch (Exception e){
            logger.error("[ClusterServiceImplTest3]prepare data error for path", e);
        }
        return "";
    }

    @Test
    public void testFindAllClusterByDcNameBind(){
        List<String> dcNameList = new LinkedList<>();
        dcNameList.add("jq");
        dcNameList.add("oy");
        dcNameList.add("fra");
        dcNameList.forEach(dcName->{
            List<ClusterTbl> result =  clusterService.findAllClusterByDcNameBind(dcName);
            if (dcName.equals("jq")){
                Assert.assertEquals(2, result.size());
            }else if (dcName.equals("oy")){
                Assert.assertEquals(2, result.size());
            }else if (dcName.equals("fra")){
                Assert.assertEquals(0, result.size());
            }
        });

    }

}
