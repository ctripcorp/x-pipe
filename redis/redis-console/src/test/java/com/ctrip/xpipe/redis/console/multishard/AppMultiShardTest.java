package com.ctrip.xpipe.redis.console.multishard;

import com.ctrip.xpipe.monitor.CatConfig;
import com.ctrip.xpipe.redis.console.AbstratAppTest;
import com.ctrip.xpipe.redis.console.AppTest;
import com.ctrip.xpipe.redis.console.healthcheck.HealthChecker;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         May 12, 2017
 */
@SpringBootApplication
public class AppMultiShardTest extends AbstratAppTest {

    private final int shardCount = 32;
    private final long []dcIds = new long[]{1, 2};
    private final long clusterId = 1;
    private final long [][]dcKeepers = new long[][]{
            new long[]{1,2,3},
            new long[]{4,5,6}
    };

    private final long beginDcClusterId = 1;
    private List<Long> dcClusterIds = new ArrayList<>();

    private final long beginShardId = 1;

    private final long beginDcClusterShardId = 1;

    private List<Long> dcClusterShardIds = new ArrayList<>();


    private String clusterName = "cluster1";
    private String shardName = "shard";


    @Before
    public void startUp() throws SQLException, ComponentLookupException {

        System.setProperty(HealthChecker.ENABLED, "false");
        System.setProperty(CatConfig.CAT_ENABLED_KEY, "false");

        prepareData();

    }


    @Test
    public void startConsole8080() throws IOException {
        System.setProperty("server.port", "8080");
        start();
    }

    private void start() throws IOException {
        SpringApplication.run(AppTest.class);
    }

    private void prepareData() throws ComponentLookupException, SQLException {

        insertCluster();
        insertShards();
        insertRedisAndKeepers();
    }

    private void insertRedisAndKeepers() throws ComponentLookupException, SQLException {

        long instanceId = 1;
        final int  redisCount = 2;
        long initRedisPort = 10000;
        long initKeeperPort = 20000;

        for(int i=0;i<dcClusterShardIds.size();i++){

            final long currentDcClusterShardId = dcClusterShardIds.get(i);
            final long currentDcIndex = i%dcIds.length;

            int initBeginRedisPort = (int) (initRedisPort + (i%dcIds.length * 1000));
            int initBeginKeeperPort = (int) (initKeeperPort + (i%dcIds.length * 1000));

            int redisPort = initBeginRedisPort + i/dcIds.length * redisCount;
            int keeperPort = initBeginKeeperPort + i/dcIds.length * redisCount;
            //insertRedis
            for(int j=0;j<redisCount;j++){

                int isMaster = j == 0?1:0;
                String insertRedis = String.format(
                        "insert into REDIS_TBL " +
                                "(id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values" +
                                "(%d,'unknown',%d,'127.0.0.1',%d,'redis',%d,0,null);", instanceId++, currentDcClusterShardId, redisPort++, isMaster);
                executeSqlScript(insertRedis);
             }

            //insertKeeper;
            for(int j=0;j<redisCount;j++){

                String insertKeeper = String.format("insert into REDIS_TBL " +
                        "(id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values " +
                        "(%d,'ffffffffffffffffffffffffffffffffffffffff',%d,'127.0.0.1',%d,'keeper',0,-1, %d);"
                        , instanceId++, currentDcClusterShardId, keeperPort++,dcKeepers[(int) currentDcIndex][j]
                );
                executeSqlScript(insertKeeper);
            }

        }
    }

    private void insertShards() throws ComponentLookupException, SQLException {

        long currentShardId = beginShardId;
        long currentDcClusterShardId = beginDcClusterShardId;

        for(int i=0; i < shardCount ; i++){

            String shardName = gtShardName(i);
            String insertShard = String.format(
                    "insert into SHARD_TBL " +
                    "(id,shard_name,cluster_id) values (%d, '%s', %d);", currentShardId, shardName, clusterId);
            executeSqlScript(insertShard);

            for(Long dc_cluster_id : dcClusterIds){

                String insertDcClusterShard = String.format(
                        "insert into DC_CLUSTER_SHARD_TBL " +
                        "(dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) " +
                        "values (%d,%d,%d,1,1);", currentDcClusterShardId, dc_cluster_id, currentShardId);
                executeSqlScript(insertDcClusterShard);
                dcClusterShardIds.add(currentDcClusterShardId);
                currentDcClusterShardId++;
            }
            currentShardId++;
        }
    }

    private String gtShardName(int index) {

        return shardName + "-" + index;
    }

    private void insertCluster() throws ComponentLookupException, SQLException {

        String insertCluster = String.format(
                "insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested) " +
                "values (%d,'%s',%d,'Cluster:cluster1 , ActiveDC : A','0000000000000000','Normal',1);", clusterId, clusterName, dcIds[0]);

        executeSqlScript(insertCluster);

        long dc_cluster_id = beginDcClusterId;

        for(long dcId : dcIds){
            String insertDcCluster = String.format(
                    "insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) "
                    + "values (%d,%d,%d,1,0);", dc_cluster_id, dcId, clusterId);
            executeSqlScript(insertDcCluster);
            dcClusterIds.add(dc_cluster_id);
            dc_cluster_id++;
        }
    }

    @After
    public void afterAppTest() throws IOException {
        waitForAnyKeyToExit();
    }

}
