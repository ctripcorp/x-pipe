package com.ctrip.xpipe.redis.console.keeperops;

import java.io.IOException;

/**
 * Local console (port 8080) wired to REAL ZK + MetaServer + KeeperContainers for an end-to-end keeper-ops run.
 * Run explicitly (not in AllTests):
 * <pre>
 * mvn -o test -pl redis/redis-console -Dtest=KeeperOpsE2EConsoleTest#startConsole8080 -DfailIfNoTests=false -Djacoco.skip=true
 * </pre>
 * Stop: create {@code ${java.io.tmpdir}/keeper-ops-console.stop}.
 *
 * <p>Data on top of apptest.sql, cluster {@code cluster1}, shard {@code shard1}. A logical BU lives in one IDC:
 * BU 44 {@code e2e-bu-44} in jq, BU 45 {@code e2e-oy-44} in oy.
 * <pre>
 * jq (dc 1, MetaServer 9747, keeper AppTest start7080 / 7081 / 7082 / 7083)
 * KC 1   127.0.0.1:7080  TFS_1   BU 44   keeper 127.0.0.1:6000  keeper_priority 2
 * KC 2   127.0.0.1:7081  TFS_1   BU 44   keeper 127.0.0.1:6001  keeper_priority 2
 * KC 3   127.0.0.1:7082  DEFAULT         keeper 127.0.0.1:6002  keeper_priority 1
 * KC 150 127.0.0.1:7083  DEFAULT         keeper 127.0.0.1:6003  keeper_priority 1
 *
 * oy (dc 2, MetaServer 9748, keeper AppTest start7180 / 7181 / 7182 / start7183)
 * KC 4   127.0.0.1:7180  TFS_1   BU 45   keeper 127.0.0.1:6100  keeper_priority 2
 * KC 5   127.0.0.1:7181  TFS_1   BU 45   keeper 127.0.0.1:6101  keeper_priority 2
 * KC 6   127.0.0.1:7182  DEFAULT         keeper 127.0.0.1:6102  keeper_priority 1
 * KC 151 127.0.0.1:7183  DEFAULT         keeper 127.0.0.1:6103  keeper_priority 1
 * </pre>
 * BU 44 / 45 priority starts NULL, so TFS keepers use keeper_priority 2 and TFS is active in each IDC.
 * switchBM on BU 44 sets its priority to 0 and only jq's MetaServer (9747) re-elects a BM keeper;
 * switchBM on BU 45 does the same in oy (9748).
 * All other keepers on these containers are removed so only shard1 is managed by them.
 */
public class KeeperOpsE2EConsoleTest extends KeeperOpsLocalConsoleTest {

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/apptest.sql") + "\n"
                + "delete from REDIS_TBL where redis_role='keeper' and keepercontainer_id in (1,2,3,4,5,6) and id not in (1,2,6,7);\n"
                + "insert into LOGICAL_BU_TBL (id, name, tfs_fs_id, active, description) values (44,'e2e-bu-44','tfs-fs-44',1,'keeper-ops e2e');\n"
                // jq
                + "update KEEPERCONTAINER_TBL set keepercontainer_disk_type='TFS_1', logical_bu_id=44 where keepercontainer_id in (1,2);\n"
                + "update KEEPERCONTAINER_TBL set keepercontainer_disk_type='DEFAULT', logical_bu_id=0 where keepercontainer_id=3;\n"
                + "insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active,keepercontainer_org_id,keepercontainer_disk_type) "
                + "values (150,1,'127.0.0.1',7083,1,1,'DEFAULT');\n"
                + "update REDIS_TBL set keeper_priority=2 where id in (1,2);\n"
                + "insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id,keeper_priority) "
                + "values (9001,'ffffffffffffffffffffffffffffffffffffffff',1,'127.0.0.1',6002,'keeper',0,0,3,1);\n"
                + "insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id,keeper_priority) "
                + "values (9002,'ffffffffffffffffffffffffffffffffffffffff',1,'127.0.0.1',6003,'keeper',0,0,150,1);\n"
                // oy: same shard1 (dc_cluster_shard_id 2), 2 TFS + 2 BM; a logical BU lives in one IDC, so oy has its own BU 45
                + "insert into LOGICAL_BU_TBL (id, name, tfs_fs_id, active, description) values (45,'e2e-oy-44','tfs-fs-oy-44',1,'keeper-ops e2e oy');\n"
                + "update KEEPERCONTAINER_TBL set keepercontainer_disk_type='TFS_1', logical_bu_id=45 where keepercontainer_id in (4,5);\n"
                + "update KEEPERCONTAINER_TBL set keepercontainer_disk_type='DEFAULT', logical_bu_id=0 where keepercontainer_id=6;\n"
                + "insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active,keepercontainer_org_id,keepercontainer_disk_type) "
                + "values (151,2,'127.0.0.1',7183,1,1,'DEFAULT');\n"
                + "update REDIS_TBL set keeper_priority=2 where id in (6,7);\n"
                + "insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id,keeper_priority) "
                + "values (9003,'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',2,'127.0.0.1',6102,'keeper',0,0,6,1);\n"
                + "insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id,keeper_priority) "
                + "values (9004,'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',2,'127.0.0.1',6103,'keeper',0,0,151,1);\n";
    }
}
